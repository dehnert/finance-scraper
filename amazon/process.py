#!/usr/bin/env python3
"""Script to match Amazon purchases to GnuCash"""

import argparse
import collections
import csv
from dataclasses import dataclass, field
import datetime
from decimal import Decimal
import itertools
import logging
import pprint
import re

from typing import Dict, Iterable, List, Optional, TypeVar

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


@dataclass(order=True)
class LineItem:
    """Single Amazon purchase line item"""
    cost: Decimal
    date: datetime.datetime  # Use the closest available approximation to when charged
    desc: str
    is_gift: bool
    data: dict = field(repr=False)  # Underlying CSV/JSON/etc. data for this line item

    @classmethod
    def from_amazon_csv(cls, item):
        """Create line item from (dict of an) Amazon CSV file"""
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
        is_gift = (item['Gift Message'] != 'Not Available') # pylint:disable=superfluous-parens
        return cls(cost=cost, date=date, desc=desc, is_gift=is_gift, data=item)

@dataclass
class Order:
    """Amazon order"""
    order_id: str
    items: list[LineItem] = field(default_factory=list)
    payments: list = field(default_factory=list)
    total: Decimal = Decimal(0)
    date: Optional[datetime.datetime] = None

    def update_payment(self, balance_payments):
        """Update fields of Order based on the LineItems

        - Compute the total
        - Compute the date (first shipping date)
        - Allocate payments between credit card and gift card
        """
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
        def sort_key(acct):
            return (((acct == 'Gift Certificate/Card') ^ (gift_amount > 0)), acct)
        for instrument in sorted(payments, key=sort_key):
            if 'Gift Certificate/Card' == instrument and gift_amount > 0:
                amount = gift_amount
            else:
                amount = balance
            self.payments.append((instrument, amount))
            balance -= amount
        assert balance == 0, f"{self.payments=} {balance_payment=}"

AMAZON_BALANCE_PAYMENT  = re.compile(r"Payment towards Amazon.com order \(‎?(?P<num>[0-9-]*)\)")
AMAZON_BALANCE_GC_CLAIM = re.compile(r"Gift card claim \(claim code xxxx-xxxxxx-(?P<code>[A-Z0-9]{4})\)") # pylint:disable=line-too-long
AMAZON_BALANCE_RELOAD   = re.compile(r"Balance Reload \(‎?(?P<num>[0-9-]*)\)")


def load_amazon_balance(bal_fp) -> BalanceActivities:
    """Return Amazon gift card balances by parsing CSV file"""
    reader = csv.DictReader(bal_fp, dialect=csv.excel_tab)
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
        assert event, f'unknown balance activity: {desc}'
        event_date = datetime.datetime.strptime(line['Date '].strip(), '%B %d, %Y').date()
        amount = comma_decimal(line['Amount'].replace('$', ''))
        activity = BalanceActivity(event_date, desc, event, arg, amount)
        activities.append(activity)
        if event == 'payment':
            payments[arg] = activity
        if event == 'reload':
            reload[arg] = activity
    return BalanceActivities(activities, payments, reload)


def load_amazon_orders(order_fp, balance_payments) -> Dict[str,Order]:
    """Return Amazon orders by parsing CSV file

    The CSV file can be requested from:
    https://www.amazon.com/hz/privacy-central/data-requests/preview.html
    It can take a few days to generate, so don't wait until you really
    need to do this import.
    """
    reader = csv.DictReader(order_fp)
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
            order = Order(order_id)
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

IGNORED_CREDITCARD_ACCOUNTS = [
    '1008',     # Two AmEx accounts, fortunately little-used
]

def get_creditcard_accounts(root: gnucash.Account) -> Dict[str,gnucash.Account]:
    """Build a dict of last-4-digits to GnuCash Account objects"""
    accts = root.lookup_by_full_name('Credit Cards').get_descendants()
    acct_map = {}
    for acct in accts:
        match = ACCT_NUM_REGEX.search(acct.GetName())
        if match:
            num = match.group('num')
            if num in IGNORED_CREDITCARD_ACCOUNTS:
                LOGGER.debug("Ignoring known-dup account number: %s", acct.GetName())
                continue
            assert num not in acct_map
            acct_map[match.group('num')] = acct #AccountInfo(acct, [])
        else:
            LOGGER.debug("Unknown last-4 for account: %s", acct.GetName())
    gift_name = 'Assets.Current Assets.Gift cards.Amazon balance'
    acct_map['Gift Certificate/Card'] = root.lookup_by_full_name(gift_name)
    return acct_map

def gnc_to_decimal(amt: gnucash.GncNumeric) -> Decimal:
    """Convert GncNumeric to a Decimal"""
    return Decimal(amt.num()) / amt.denom()

def set_split_amount(split, amt):
    """Set the amount/value of a GnuCash split

    Converts from a Python number to GncNumeric."""
    num, denom = Decimal(amt).as_integer_ratio()
    gnc = gnucash.GncNumeric(num, denom)
    split.SetValue(gnc)
    LOGGER.info('amt=%s gnc=%s split=%s memo=%s', amt, gnc_to_decimal(gnc),
                gnc_to_decimal(split.GetValue()), split.GetMemo())
    LOGGER.info("set split: %s", split_tuple(split))

def split_tuple(split):
    """Helper function to convert a split into a printable tuple"""
    splits = [(id(split1), split1.GetAccount().get_full_name(), split1.GetValue().to_double())
              for split1 in split.parent.GetSplitList()]
    return split.parent.GetDate(), len(split.parent.GetSplitList()), splits

def trans_tuple(trans):
    """Helper function to convert a split into a printable tuple"""
    splits = [(id(split1), split1.GetAccount().get_full_name(), split1.GetValue().to_double())
              for split1 in trans.GetSplitList()]
    return trans.GetDate(), len(trans.GetSplitList()), splits

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

@dataclass
class AccountMap:
    """Significant accounts for import

    cc: mapping of credit card numbers to accounts
    source: source of splits to replace (generally Imbalance-USD)
    tag: special account to for tagging that these were auto-imported
    gift: account to assign gift line items to
    expense: default account to assign line items to
    """
    cc: Dict[str,gnucash.Account] # pylint:disable=invalid-name
    source: gnucash.Account
    tag: gnucash.Account
    gift: gnucash.Account
    expense: gnucash.Account


DEBUG_ORDER_ID: List[str] = []

def match_order__find_payment(order, amount, candidates, ):
    """match_order: find a payment given a list of candidates"""
    matched = []
    for match_split in candidates:
        match_amount = -1 * gnc_to_decimal(match_split.GetValue())
        if order.order_id in DEBUG_ORDER_ID:
            LOGGER.info('comparing %s?=%s to %s', amount, match_amount,
                        split_tuple(match_split))
        if (amount == match_amount and
            near_date(order.date, match_split.parent.GetDate())):
            # Splits match!
            LOGGER.debug("matched transaction: %s=%s, %s to %s",
                         amount, match_amount, order, split_tuple(match_split))
            matched.append(match_split)
    matched_split = None
    if len(matched) == 0:
        # boring
        pass
    elif len(matched) == 1:
        # Yay
        matched_split = matched[0]
    else:
        LOGGER.warning("multiple possible transactions: %s %s",
                       order, [split_tuple(split) for split in matched])
    return matched_split

Elem = TypeVar('Elem')
def iter_subseq(lst: List[Elem]) -> Iterable[Iterable[Elem]]:
    """Return all proper subsequences of `iterable`

    We do not include the empty subsequence or the whole list. Results are
    ordered by length, and for a given length lexicographic based on the input
    iterable (using itertools.combinations internally).  """
    def subset_n_fn(n):
        return itertools.combinations(lst, n)
    lengths = range(1, len(lst))
    return itertools.chain.from_iterable(map(subset_n_fn, lengths))


@dataclass
class PaymentMatch:
    """Return value elem from match_order__find_payments"""
    amount: Decimal
    anchor: gnucash.Split # existing split to anchor transaction
    # - payment splits list, which should either
    #   - parallel order.payments, with entries for splits that need to be
    #     created left as None
    #   - empty, indicating no new splits need be created
    create: List[Optional[gnucash.Split]]
    items: List[LineItem]

def match_order__find_payments_part(order: Order,
                                    candidates: List[gnucash.Split]) -> List[PaymentMatch]:
    """Find payments that match a partition of the line items"""
    subsets = iter_subseq(order.items)
    matches: List[PaymentMatch] = []
    for order_subset in subsets:
        amount = Decimal(0) + sum(item.cost for item in order_subset)
        matched_split = match_order__find_payment(order, amount, candidates)
        if matched_split:
            matches.append(PaymentMatch(amount, matched_split, [], list(order_subset)))

    item_concat = list(itertools.chain.from_iterable(map((lambda part: part.items), matches)))
    amount_sum = sum(part.amount for part in matches)
    if (sorted(item_concat) == sorted(order.items) and
        amount_sum == order.total):
        return matches

    if matches:
        # Got at least some matches, so worth mentioning
        LOGGER.warning("couldn't match order: %s %s", order, matches)

    return []

def match_order__find_payments(order, cc_accts, acct_trans) -> List[PaymentMatch]:
    """match_order: Find candidate payments

    Returns a list of PaymentMatch objects
    """
    payment_splits: List[gnucash.Split] = []
    payment_split = None
    if order.order_id in DEBUG_ORDER_ID:
        LOGGER.info('order=%s', order)
    for instrument, amount in order.payments:
        other_acct = match_account(instrument, cc_accts)
        if not other_acct:
            LOGGER.warning('unknown account %s on %s %s', instrument, order.date, order.order_id)
            payment_splits.append(None)
            continue # next payment
        other_acct_name = other_acct.get_full_name()
        candidates = acct_trans[other_acct_name]

        matched_split = match_order__find_payment(order, amount, candidates)
        if matched_split:
            payment_split = matched_split
        payment_splits.append(matched_split)

    if payment_split:
        # Yay we found a split that matches everything
        for split in payment_splits:
            # We should match at most one split
            assert split == payment_split or split is None, \
                   (f'multiple payments: {order.payments=}, {split=}, '
                    f'{payment_splits=}, {payment_split=}')
        return [PaymentMatch(order.total, payment_split, payment_splits, order.items)]

    if len(order.items) < 2:
        # With only one item, taking subsets won't help
        return []

    if len(order.payments) != 1:
        # Multiple payments make things too complicated for now
        # We could probably do something once we have an example
        return []

    # pylint:disable-next=undefined-loop-variable
    if not (instrument and other_acct):
        # We didn't find the account earlier, we're not going to find it now
        return []

    # These should all be valid still, but if we support multiple payments
    # we'll need to do something
    #other_acct = match_account(instrument, cc_accts)
    #other_acct_name = other_acct.get_full_name()
    #candidates = acct_trans[other_acct_name]

    return match_order__find_payments_part(order, candidates)


def match_order__update_splits(order, match, acct_map):
    """Update GnuCash splits for matched order

    We perform four operations:
    - Remove unneeded splits from the existing transaction. The existing
      transaction is identified based on `match.anchor`, which is the
      anchoring split from the target account. Any splits on the transaction
      associated with the `acct_map.source` account are deleted.
    - Add a tag split to `acct_map.tag`
    - Add payment splits -- `match.create` (if non-empty) should parallel
      `order.payments` and have pre-existing splits that don't need to be
      created. If `match.create` is empty, no new payments will be created.
    - Add a purchase split for each line item

    """

    remove_splits = []
    if match.anchor:
        # There's an existing split for payment
        transaction = match.anchor.parent
        LOGGER.info("pre-update transaction: %s", trans_tuple(transaction))
        for split in transaction.GetSplitList():
            if split.GetAccount().get_full_name() == acct_map.source.get_full_name():
                remove_splits.append(split)
    else:
        # Either this order has not yet been imported, it's gift-card only so
        # doesn't show up in CC imports, or it's been imported and already
        # moved away. In the first case we want to wait, the second basically
        # doesn't exist, and the third we don't want to act again. Either way,
        # ignore.
        # For the CC case, note that we *do* check if there's a transaction
        # from the gift card account to the source account, so manually
        # adding such transactions and then running this script will create
        # the splits appropriately.
        return

    book = acct_map.source.get_book()
    # Add a tag split to mark the import
    split = gnucash.Split(book)
    split.SetParent(transaction)
    split.SetMemo(f'auto-imported Amazon purchase {order.order_id}')
    split.SetAccount(acct_map.tag)
    set_split_amount(split, 0)

    # Add payment splits
    for (instrument, amount), split in zip(order.payments, match.create):
        if split:
            # Already set up
            continue
        other_acct = match_account(instrument, acct_map.cc)
        split = gnucash.Split(book)
        split.SetParent(transaction)
        split.SetMemo('payment')
        split.SetAccount(other_acct)
        set_split_amount(split, -1*amount)

    # Add purchase splits
    for item in match.items:
        split = gnucash.Split(book)
        split.SetParent(transaction)
        split.SetMemo(item.desc)
        split.SetAccount(acct_map.gift if item.is_gift else acct_map.expense)
        set_split_amount(split, item.cost)

    for split in remove_splits:
        split.Destroy()
    LOGGER.info("updated transaction: %s", trans_tuple(transaction))
    assert transaction.IsBalanced()


def match_order(order, acct_map: AccountMap, acct_trans, ):
    """Match a single Amazon order with GnuCash splits"""
    matches = match_order__find_payments(order, acct_map.cc, acct_trans)
    for match in matches:
        match_order__update_splits(order, match, acct_map)


def match_splits(acct_map, orders):
    """Match Amazon orders with GnuCash splits"""
    # Pylint isn't wrong about this, but we'll fix it when refactoring match_order
    splits = acct_map.source.GetSplitList()

    # Note: match_splits is quadratic: for each order we loop over each split
    # associated with the right account. We know the amounts and a rough time
    # window (and require that to be a unique key -- otherwise we don't match
    # orders to splits), so if we had performance problems we could index based
    # on that data. In practice, we expect perhaps a few dozen splits at a
    # time, so don't bother with the complexity. Our concession to performance
    # is the acct_trans map, so we're not looping over splits from *every*
    # account.

    # Build a map of CC account to CC splits that need to be matched
    acct_trans = {cc_acct.get_full_name(): [] for cc_acct in acct_map.cc.values()}
    for split in splits:
        LOGGER.info("trying to match imbalance: %s", split_tuple(split))
        for other_split in split.parent.GetSplitList():
            acct_trans.get(other_split.GetAccount().get_full_name(), []).append(other_split)

    for _order_id, order in orders.items():
        match_order(order, acct_map, acct_trans, )


def gnucash_import(filename, orders):
    """Handle all the GnuCash needing bits of this process

    (Open the file, find credit card accounts, match up orders)
    """
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
        acct_map = AccountMap(cc=accts, source=imbalance, tag=tag_acct,
                              gift=gift_acct, expense=expense_acct, )
        match_splits(acct_map, orders)

def main():
    """Do everything"""
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG)
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
