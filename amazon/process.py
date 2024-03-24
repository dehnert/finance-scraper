#!/usr/bin/env python3

import csv
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
import logging
import pprint
import sys

from typing import Dict, List, Any

import gnucash

LOGGER = logging.getLogger(__name__)

def comma_decimal(string):
    """Given an amount as a string using commas as thousands separators, return a Decimal"""
    return Decimal(string.replace(',', ''))

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
    items: list[LineItem]
    payments: list
    total: Decimal

    def update_payment(self):
        self.total = sum(item.cost for item in self.items)
        item = self.items[0]
        payment = item.data['Payment Instrument Type']
        payments = payment.split(' and ')
        print(self.total)
        self.payments = [(instrument, Decimal(self.total if i == 0 else 0)) for i, instrument in enumerate(payments)]

def load_csv(fp) -> Dict[str,Order]:
    reader = csv.DictReader(fp)
    orders = {}
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
            order = Order([], [], Decimal(0))
            orders[order_id] = order
        order.items.append(LineItem.from_amazon_csv(line))
    for order_id, order in orders.items():
        order.update_payment()
    return orders

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    data = load_csv(sys.stdin)
    print(pprint.pformat(data, width=120))
