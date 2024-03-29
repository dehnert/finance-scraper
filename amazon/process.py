#!/usr/bin/env python3

import argparse
import collections
import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
import logging
import pprint
import re
import sys

from typing import Any, Dict, List, Tuple

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
    date: date
    desc: str
    event: str  # enum would be better
    arg: str
    amount: Decimal

@dataclass
class LineItem:
    cost: Decimal
    date: datetime  # Use the closest available approximation to when charged
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
        # (https://www.amazon.com/gp/css/summary/print.html/ref=ppx_yo_dt_b_invoice_o01?ie=UTF8&orderID=114-6128463-0855459)
        # they get charged to credit cards at a level that's coarser than
        # Shipment Item Subtotal (or the Ship Date or Carrier Name & Tracking
        # Number) but finer than the order number, and I didn't find the
        # quantities in question in any of the CSVs available to me.

        cost = comma_decimal(item['Total Owed'])

        # Updated in 3.11 to support more formats:
        # https://docs.python.org/3/library/datetime.html#datetime.datetime.fromisoformat
        # Unfortunately, Ubuntu 22.04 has 3.10 by default and for GnuCash
        # Stripping off the Z timezone indicator makes it parse though
        date = datetime.fromisoformat(item['Ship Date'].split(' and ')[0].replace('Z', ''))

        desc = item['Product Name']
        is_gift = (item['Gift Message'] != 'Not Available')
        return cls(cost=cost, date=date, desc=desc, is_gift=is_gift, data=item)

@dataclass
class Order:
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
AMAZON_BALANCE_GC_CLAIM = re.compile(r"Gift card claim \(claim code xxxx-xxxxxx-(?P<code>[A-Z0-9]{4})\)")
AMAZON_BALANCE_RELOAD   = re.compile(r"Balance Reload \(‎?(?P<num>[0-9-]*)\)")


def load_amazon_balance(fp) -> Tuple[List[BalanceActivity], Dict[str,BalanceActivity]]:
    reader = csv.DictReader(fp, dialect=csv.excel_tab)
    activities: List[BalanceActivity] = []
    payments: Dict[str,BalanceActivity] = {}
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
        event_date = datetime.strptime(line['Date '].strip(), '%B %d, %Y').date()
        amount = comma_decimal(line['Amount'].replace('$', ''))
        activity = BalanceActivity(event_date, desc, event, arg, amount)
        activities.append(activity)
        if event == 'payment':
            payments[arg] = activity
    return activities, payments


def load_amazon_orders(fp, balance_activity) -> Dict[str,Order]:
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
    for order_id, order in orders.items():
        order.update_payment(balance_activity)
        payment_count[len(order.payments)] += 1
        for payment, amount in order.payments:
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
    acct_map['Gift Certificate/Card'] = root.lookup_by_full_name('Assets.Current Assets.Gift cards.Amazon balance')
    return acct_map

def gnc_to_decimal(amt):
    assert amt.denom() == 100
    return Decimal(amt.num()) / amt.denom()

def split_tuple(split):
    """Helper function to convert a split into a printable tuple"""
    splits = [(split1.GetAccount().get_full_name(), split1.GetAmount().to_double()) for split1 in split.parent.GetSplitList()]
    return split.parent.GetDate(), len(split.parent.GetSplitList()), splits

def match_account(instrument, accts):
    """Match account based on payment instrument from import to candidate accounts"""
    acct = accts.get(instrument)
    if acct: return acct
    _network, _dash, num = instrument.partition(' - ')
    return accts.get(num)

def near_date(order_date, cc_date):
    """See if order date and credit card transaction date are plausibly close"""
    return ((order_date - timedelta(days=2) <= cc_date) and
            (order_date + timedelta(days=14) > cc_date))

def match_splits(acct, cc_accts, orders):
    splits = acct.GetSplitList()

    # Build a map of CC account to CC splits that need to be matched
    acct_trans = {cc_acct.get_full_name(): [] for cc_acct in cc_accts.values()}
    for split in splits:
        LOGGER.info(split_tuple(split))
        for other_split in split.parent.GetSplitList():
            acct_trans.get(other_split.GetAccount().get_full_name(), []).append(other_split)

    for order_id, order in orders.items():
        if len(order.payments) == 1:
            #LOGGER.info("considering order %s", order)
            instrument, amount = order.payments[0]
            other_acct = match_account(instrument, cc_accts)
            if not other_acct:
                LOGGER.warning('unknown account %s', instrument)
                continue # next order
            other_acct_name = other_acct.get_full_name()
            match_splits = acct_trans[other_acct_name]
            for match_split in match_splits:
                match_amount = -1 * gnc_to_decimal(match_split.GetAmount())
                if (amount == match_amount and
                    near_date(order.date, match_split.parent.GetDate())):
                    # Splits match!
                    LOGGER.info("matched transaction: %s=%s, %s to %s",
                                amount, match_amount, order, split_tuple(match_split))
        else:
            LOGGER.info("ignoring order with multiple payments %s: %s", order.payments, order)
            # Same basic algorithm, except any smaller payment is reasonable
            # (but the gift card balancing split we invent)

def gnucash_import(filename, orders):
    book_uri = 'file://' + filename
    mode = gnucash.SessionOpenMode.SESSION_NORMAL_OPEN
    mode = gnucash.SessionOpenMode.SESSION_READ_ONLY
    with gnucash.Session(book_uri=book_uri, mode=mode) as session:
        root = session.get_book().get_root_account()
        accts = get_creditcard_accounts(root)
        LOGGER.info('CC accounts %s', accts)
        imbalance = root.lookup_by_full_name('Imbalance-USD')
        match_splits(imbalance, accts, orders)

def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG)
    if args.amazon_balance:
        balance_activities, balance_payments = load_amazon_balance(args.amazon_balance)
    else:
        balance_activities = []
        balance_payments = {}
    print(pprint.pformat(balance_activities, width=120))
    orders = load_amazon_orders(args.amazon_orders, balance_payments)
    print(pprint.pformat(orders, width=120))
    gnucash_import(args.gnucash, orders)

if __name__ == '__main__':
    main()
