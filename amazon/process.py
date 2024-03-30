#!/usr/bin/env python3
"""Script to match Amazon purchases to GnuCash"""

import argparse
import collections
import csv
from dataclasses import dataclass
import datetime
from decimal import Decimal
import logging
import pprint
import re

from typing import Dict, List

import gnucash # type: ignore

LOGGER = logging.getLogger(__name__)

def parse_args():
    """Parse arguments"""
    parser = argparse.ArgumentParser(description="Load Amazon transactions into GnuCash")
    # https://www.amazon.com/hz/privacy-central/data-requests/preview.html
    # It can take a few days to generate, so don't wait until you really
    # need to do this import.
    parser.add_argument('--amazon-orders', type=argparse.FileType('r'),
                        help="Retail.OrderHistory file", )
    # Make sure to include table header
    parser.add_argument('--amazon-balance', type=argparse.FileType('r'),
                        help="Gift card balance (as TSV) copy/pasted from "
                             "https://www.amazon.com/gc/balance")
    parser.add_argument('--gnucash', type=str,
                        help='path to gnucash file')
    return parser.parse_args()


def comma_decimal(string):
    """Given an amount as a string using commas as thousands separators, return a Decimal"""
    return Decimal(string.replace(',', ''))

@dataclass
class BalanceActivity:
    """Class to represent a single Amazon gift balance transaction"""
    date: datetime.date
    desc: str
    event: str  # enum would be better
    arg: str
    amount: Decimal

@dataclass
class BalanceActivities:
    """Class to represent Amazon gift balance transactions (collectively)"""
    data: List[BalanceActivity]
    redemption: Dict[str,BalanceActivity]
    reload: Dict[str,BalanceActivity]


@dataclass
class LineItem:
    """Single Amazon purchase line item"""
    cost: Decimal
    date: datetime.datetime  # Use the closest available approximation to when charged
    desc: str
    is_gift: bool
    data: dict      # Underlying CSV/JSON/etc. data for this line item

    @classmethod
    def from_amazon_csv(cls, item):
        # Money related columns for Amazon:
        # Total Owed appears to be the total of all the per-line item costs
        # So (Unit Price + Unit Price Tax)*Quantity + Shipping Charge + Total Discounts

        # Shipment Item Subtotal and Shipment Item Subtotal Tax relate to
        # things that get shipped together, which aren't single orders and I'm
        # not sure are terribly useful to think about. Judging by order
        # 114-6128463-0855459
        # (https://www.amazon.com/gp/css/summary/print.html/ref=ppx_yo_dt_b_invoice_o01?ie=UTF8&orderID=114-6128463-0855459) #pylint:disable=line-too-long
        # they get charged to credit cards at a level that's coarser than
        # Shipment Item Subtotal (or the Ship Date or Carrier Name & Tracking
        # Number) but finer than the order number, and I didn't find the
        # quantities in question in any of the CSVs available to me.

        cost = comma_decimal(item['Total Owed'])

        # Updated in 3.11 to support more formats:
        # https://docs.python.org/3/library/datetime.html#datetime.datetime.fromisoformat
        # Unfortunately, Ubuntu 22.04 has 3.10 by default and for GnuCash
        # Stripping off the Z timezone indicator makes it parse though
        date = datetime.datetime.fromisoformat(item['Ship Date'].split(' and ')[0].replace('Z', ''))

        desc = item['Product Name']
        is_gift = (item['Gift Message'] != 'Not Available')
        return cls(cost=cost, date=date, desc=desc, is_gift=is_gift, data=item)

@dataclass
class Order:
    """Amazon order"""
    order_id: str
    items: list[LineItem]
    payments: list
    total: Decimal

    def update_payment(self, balance_payments):
        self.total = sum(item.cost for item in self.items)
        self.date = min(item.date for item in self.items)
        item = self.items[0]
        payment = item.data['Payment Instrument Type']
        payments = payment.split(' and ')
        assert len(payments) <= 2

        # Find gift card payments, and allocate payments between credit card
        # and gift card balances as we can
        balance_payment = balance_payments.get(self.order_id)
        if balance_payment:
            assert 'Gift Certificate/Card' in payments
            gift_amount = balance_payment.amount * -1
        else:
            gift_amount = 0
        balance = self.total
        self.payments = []
        key = lambda x: (((x == 'Gift Certificate/Card') ^ (gift_amount > 0)), x)
        for instrument in sorted(payments, key=key):
            if 'Gift Certificate/Card' == instrument and gift_amount > 0:
                amount = gift_amount
            else:
                amount = balance
            self.payments.append((instrument, amount))
            balance -= amount
        assert balance == 0, "payments=%s balance_payment=%s" % (self.payments, balance_payment)

AMAZON_BALANCE_PAYMENT  = re.compile(r"Payment towards Amazon.com order \(‎?(?P<num>[0-9-]*)\)")
AMAZON_BALANCE_GC_CLAIM = re.compile(r"Gift card claim \(claim code xxxx-xxxxxx-(?P<code>[A-Z0-9]{4})\)") # pylint:disable=line-too-long
AMAZON_BALANCE_RELOAD   = re.compile(r"Balance Reload \(‎?(?P<num>[0-9-]*)\)")


def load_amazon_balance(fp) -> BalanceActivities:
    reader = csv.DictReader(fp, dialect=csv.excel_tab)
    activities: List[BalanceActivity] = []
    payments: Dict[str,BalanceActivity] = {}
    reload: Dict[str,BalanceActivity] = {}
    for line in reader:
        event = None
        desc = line['Description '].strip()
        match = AMAZON_BALANCE_PAYMENT.match(desc)
        if match:
            event = 'payment'
            arg = match.group('num')
        match = AMAZON_BALANCE_GC_CLAIM.match(desc)
        if match:
            event = 'gc-claim'
            arg = match.group('code')
        match = AMAZON_BALANCE_RELOAD.match(desc)
        if match:
            event = 'reload'
            arg = match.group('num')
        assert event, 'unknown balance activity: %s' % (desc, )
        event_date = datetime.datetime.strptime(line['Date '].strip(), '%B %d, %Y').date()
        amount = comma_decimal(line['Amount'].replace('$', ''))
        activity = BalanceActivity(event_date, desc, event, arg, amount)
        activities.append(activity)
        if event == 'payment':
            payments[arg] = activity
        if event == 'reload':
            reload[arg] = activity
    return BalanceActivities(activities, payments, reload)


def load_amazon_orders(fp, balance_payments) -> Dict[str,Order]:
    reader = csv.DictReader(fp)
    orders: Dict[str,Order] = {}
    for line in reader:
        if line['Order Status'] == 'Cancelled':
            LOGGER.debug("Skipping cancelled order: %s", line)
            continue
        if line['Shipment Item Subtotal'] == 'Not Available' and line['Quantity'] == '0':
            LOGGER.debug("Skipping cancelled item: %s", line)
            continue
        order_id = line['Order ID']
        order = orders.get(order_id)
        if not order:
            order = Order(order_id, [], [], Decimal(0))
            orders[order_id] = order
        order.items.append(LineItem.from_amazon_csv(line))
    payment_count: collections.Counter[int] = collections.Counter()
    payment_instruments: collections.Counter[str] = collections.Counter()
    for order in orders.values():
        order.update_payment(balance_payments)
        payment_count[len(order.payments)] += 1
        for payment, _amount in order.payments:
            payment_instruments[payment] += 1
    LOGGER.info("payment counts: %s", payment_count)
    LOGGER.info("payment instruments: %s", payment_instruments)
    return orders

# Gift card transaction history:
# https://www.amazon.com/gc/balance

# GnuCash portion

ACCT_NUM_REGEX = re.compile(r'(?P<num>\d{4})\D')

#@dataclass
#class AccountInfo:
#    acct: gnucash.Account
#    trans: List[gnucash.Transaction]

def get_creditcard_accounts(root: gnucash.Account) -> Dict[str,gnucash.Account]:
    accts = root.lookup_by_full_name('Credit Cards').get_descendants()
    acct_map = {}
    for acct in accts:
        match = ACCT_NUM_REGEX.search(acct.GetName())
        if match:
            num = match.group('num')
            if num == '1008': # Two AmEx accounts, fortunately little-used
                LOGGER.debug("Ignoring known-dup account number: %s", acct.GetName())
                continue
            assert num not in acct_map
            acct_map[match.group('num')] = acct #AccountInfo(acct, [])
        else:
            LOGGER.debug("Unknown last-4 for account: %s", acct.GetName())
    gift_name = 'Assets.Current Assets.Gift cards.Amazon balance'
    acct_map['Gift Certificate/Card'] = root.lookup_by_full_name(gift_name)
    return acct_map

def gnc_to_decimal(amt):
    return Decimal(amt.num()) / amt.denom()

def set_split_amount(split, amt):
    num, denom = Decimal(amt).as_integer_ratio()
    gnc = gnucash.GncNumeric(num, denom)
    split.SetAmount(gnc)
    LOGGER.info('amt=%s gnc=%s split=%s memo=%s', amt, gnc_to_decimal(gnc),
                gnc_to_decimal(split.GetAmount()), split.GetMemo())
    LOGGER.info("set split: %s", split_tuple(split))

def split_tuple(split):
    """Helper function to convert a split into a printable tuple"""
    splits = [(split1.GetAccount().get_full_name(), split1.GetAmount().to_double())
              for split1 in split.parent.GetSplitList()]
    return split.parent.GetDate(), len(split.parent.GetSplitList()), splits

def match_account(instrument, accts):
    """Match account based on payment instrument from import to candidate accounts"""
    acct = accts.get(instrument)
    if acct:
        return acct
    _network, _dash, num = instrument.partition(' - ')
    return accts.get(num)

def near_date(order_date, cc_date):
    """See if order date and credit card transaction date are plausibly close"""
    return ((order_date - datetime.timedelta(days=2) <= cc_date) and
            (order_date + datetime.timedelta(days=14) > cc_date))

def match_order(order, cc_accts, acct_trans, imbalance, tag_acct, gift_acct, expense_acct, ):
    """Match a single Amazon order with GnuCash splits"""
    book = imbalance.get_book()
    payment_splits = []
    payment_split = None

    # Find candidate payments
    for instrument, amount in order.payments:
        other_acct = match_account(instrument, cc_accts)
        if not other_acct:
            LOGGER.warning('unknown account %s on %s', instrument, order.date)
            payment_splits.append(None)
            continue # next payment
        other_acct_name = other_acct.get_full_name()
        candidates = acct_trans[other_acct_name]
        matched = []
        for match_split in candidates:
            match_amount = -1 * gnc_to_decimal(match_split.GetAmount())
            if (amount == match_amount and
                near_date(order.date, match_split.parent.GetDate())):
                # Splits match!
                LOGGER.debug("matched transaction: %s=%s, %s to %s",
                             amount, match_amount, order, split_tuple(match_split))
                matched.append(match_split)
        if len(matched) == 0:
            # boring
            payment_splits.append(None)
        elif len(matched) == 1:
            # Yay
            payment_split = matched[0]
            payment_splits.append(payment_split)
        else:
            LOGGER.warning("multiple possible transactions: %s %s",
                           order, [split_tuple(split) for split in matched])
            payment_splits.append(None)

    for split in payment_splits:
        # We should match at most one split
        assert split == payment_split or split is None, \
               (f'multiple payments: {order.payments=}, {split=}, '
                f'{payment_splits=}, {payment_split=}')

    remove_splits = []
    if payment_split:
        LOGGER.info("pre-update transaction: %s", split_tuple(payment_split))
        # There's an existing split for payment
        transaction = payment_split.parent
        transaction.BeginEdit()
        for split in transaction.GetSplitList():
            LOGGER.info("iterating %s %s", split.GetAccount(), imbalance)
            if split.GetAccount().get_full_name() == imbalance.get_full_name():
                remove_splits.append(split)
                LOGGER.info('destroy')
                #payment_split.RemovePeerSplit(split)
                #split.SetParent(None)
    else:
        # Completely new transaction
        #transaction = gnucash.Transaction(book)
        #transaction.SetDate(order.date)
        #transaction.SetDescription("Amazon purchase")

        # Either this order has not yet been imported, it's gift-card only so
        # doesn't show up in CC imports, or it's been imported and already
        # moved away. In the first case we want to wait, the second basically
        # doesn't exist, and the third we don't want to act again. Either way,
        # ignore.
        return

    LOGGER.info("pre-update transaction: %s", split_tuple(payment_split))

    # Add a tag split to mark the import
    split = gnucash.Split(book)
    split.SetParent(transaction)
    split.SetMemo(f'auto-imported Amazon purchase {order.order_id}')
    split.SetAccount(tag_acct)
    set_split_amount(split, 0)

    # Add payment splits
    for (instrument, amount), split in zip(order.payments, payment_splits):
        if split == payment_split:
            # Already set up
            continue
        other_acct = match_account(instrument, cc_accts)
        split = gnucash.Split(book)
        split.SetParent(transaction)
        split.SetMemo('payment')
        split.SetAccount(other_acct)
        set_split_amount(split, -1*amount)

    # Add purchase splits
    for item in order.items:
        split = gnucash.Split(book)
        split.SetParent(transaction)
        split.SetMemo(item.desc)
        split.SetAccount(gift_acct if item.is_gift else expense_acct)
        set_split_amount(split, item.cost)

    LOGGER.info("updated transaction: %s", split_tuple(payment_split))

    for split in remove_splits:
        LOGGER.info("remove split: %s %s", split, split_tuple(payment_split))
        #payment_split.RemovePeerSplit(split)
        split.SetParent(None)
        #split.Destroy()
    LOGGER.info("updated transaction: %s", split_tuple(payment_split))

    assert transaction.IsBalanced()
    transaction.CommitEdit()

def match_splits(imbalance, tag_acct, gift_acct, expense_acct, cc_accts, orders):
    """Match Amazon orders with GnuCash splits"""
    splits = imbalance.GetSplitList()

    # Build a map of CC account to CC splits that need to be matched
    acct_trans = {cc_acct.get_full_name(): [] for cc_acct in cc_accts.values()}
    for split in splits:
        LOGGER.info(split_tuple(split))
        for other_split in split.parent.GetSplitList():
            acct_trans.get(other_split.GetAccount().get_full_name(), []).append(other_split)

    for _order_id, order in orders.items():
        match_order(order, cc_accts, acct_trans, imbalance, tag_acct, gift_acct, expense_acct, )


def gnucash_import(filename, orders):
    book_uri = 'file://' + filename
    mode = gnucash.SessionOpenMode.SESSION_READ_ONLY
    mode = gnucash.SessionOpenMode.SESSION_NORMAL_OPEN
    with gnucash.Session(book_uri=book_uri, mode=mode) as session:
        root = session.get_book().get_root_account()
        accts = get_creditcard_accounts(root)
        LOGGER.info('CC accounts %s', accts)
        imbalance = root.lookup_by_full_name('Imbalance-USD')
        tag_acct = root.lookup_by_full_name('Expenses._Tag.Amazon')
        gift_acct = root.lookup_by_full_name('Expenses.Gifts')
        expense_acct = root.lookup_by_full_name('Expenses')
        match_splits(imbalance, tag_acct, gift_acct, expense_acct, accts, orders)

def main():
    args = parse_args()
    logging.basicConfig(level=logging.INFO)
    if args.amazon_balance:
        balance_activities = load_amazon_balance(args.amazon_balance)
    else:
        balance_activities = BalanceActivities([], {}, {})
    print(pprint.pformat(balance_activities, width=120))
    orders = load_amazon_orders(args.amazon_orders, balance_activities.redemption)
    print(pprint.pformat(orders, width=120))
    gnucash_import(args.gnucash, orders)

if __name__ == '__main__':
    main()
