#!/usr/bin/env bash

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

SCHEMASDIR="$CURDIR"/format_schemas

# Create and update format schemas
$CLICKHOUSE_CLIENT -m -n -q "
DROP STREAM IF EXISTS stateless_99073_complex_avro_schema;

CREATE STREAM stateless_99073_complex_avro_schema (
  \`item.TransactionID\` array(string),
  \`item.IIN_BIN\` array(string),
  \`item.LegalAddress\` array(string),
  \`item.PhysicalAddress\` array(nullable(string)),
  \`item.KkmID\` array(string),
  \`item.AnnullationStatus\` array(bool),
  \`item.RegNumbKKM\` array(string),
  \`item.FactoryNumbKKM\` array(string),
  \`item.TaxPayerActivityType\` array(enum( 'DOMAIN_TRADING' = 0, 'DOMAIN_SERVICES' = 1, 'DOMAIN_GASOIL' = 2, 'DOMAIN_HOTELS' = 3, 'DOMAIN_TAXI' = 4, 'DOMAIN_PARKING' = 5)),
  \`item.OperationType\` array(enum( 'OPERATION_BUY' = 0, 'OPERATION_BUY_RETURN' = 1, 'OPERATION_SELL' = 2, 'OPERATION_SELL_RETURN' = 3)),
  \`item.OperationDate\` array(datetime64(3)),
  \`item.CashierCode\` array(string),
  \`item.CheckSumm\` array(decimal(20, 4)),
  \`item.ReceivedSumm\` array(decimal(20, 4)),
  \`item.DeliverySumm\` array(decimal(20, 4)),
  \`item.DiscountSumm\` array(decimal(20, 4)),
  \`item.MarkupSumm\` array(decimal(20, 4)),
  \`item.Payments.Type\` array(array(enum( 'PAYMENT_CASH' = 0, 'PAYMENT_CARD' = 1, 'PAYMENT_CREDIT' = 2, 'PAYMENT_TARE' = 3, 'PAYMENT_MOBILE' = 4))),
  \`item.Payments.Sum\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.ItemType\` array(array(enum( 'ITEM_TYPE_COMMODITY' = 0, 'ITEM_TYPE_STORNO_COMMODITY' = 1, 'ITEM_TYPE_MARKUP' = 2, 'ITEM_TYPE_STORNO_MARKUP' = 3, 'ITEM_TYPE_DISCOUNT' = 4, 'ITEM_TYPE_STORNO_DISCOUNT' = 5))),
  \`item.CheckPositionList.Item.ItemsType.ItemKod\` array(array(string)),
  \`item.CheckPositionList.Item.ItemsType.ItemName\` array(array(string)),
  \`item.CheckPositionList.Item.ItemsType.SectionKod\` array(array(string)),
  \`item.CheckPositionList.Item.ItemsType.ItemCount\` array(array(int64)),
  \`item.CheckPositionList.Item.ItemsType.ItemPrice\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.Item.ItemsType.FullItemPrice\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.Item.ItemsType.ItemNDS\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.DiscountOrMarkUp.DiscountMarkUpType.DiscountOrMarkUpName\` array(array(string)),
  \`item.CheckPositionList.DiscountOrMarkUp.DiscountMarkUpType.DiscountOrMarkUpSumm\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.DiscountOrMarkUp.DiscountMarkUpType.TaxDiscountOrMarkUpItem\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.ItemStorno.ItemStornoType.ItemStornoName\` array(array(string)),
  \`item.CheckPositionList.ItemStorno.ItemStornoType.SectionCodeForStorno\` array(array(string)),
  \`item.CheckPositionList.ItemStorno.ItemStornoType.ItemStornoCount\` array(array(int64)),
  \`item.CheckPositionList.ItemStorno.ItemStornoType.StornoItemPrice\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.ItemStorno.ItemStornoType.FullStornoItemSumm\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.ItemStorno.ItemStornoType.TaxStornoItem\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.DiscountMarkupStorno.DiscountMarkupStornoType.DiscountMarkupStornoName\` array(array(string)),
  \`item.CheckPositionList.DiscountMarkupStorno.DiscountMarkupStornoType.DiscountMarkupStornoSumm\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.DiscountMarkupStorno.DiscountMarkupStornoType.TaxDiscountMarkupStorno\` array(array(decimal(20, 4))),
  \`item.CheckPositionList.MarkingCode\` array(array(nullable(string))),
  \`item.CheckPositionList.RegControlStampNum\` array(array(nullable(string))),
  \`item.CheckPositionList.ProductId\` array(array(nullable(string))),
  \`item.CheckPositionList.Gtin\` array(array(nullable(string))),
  \`item.CheckPositionList.UnitCode\` array(array(nullable(string))),
  \`item.AdlServiceAccountNumber\` array(nullable(string)),
  \`item.AdlGasOilCorrectionNumber\` array(nullable(string)),
  \`item.AdlGasOilCorrectionSum\` array(nullable(decimal(20, 4))),
  \`item.AdlGasOilCardNumber\` array(nullable(string)),
  \`item.AdlTaxiCarNumber\` array(nullable(string)),
  \`item.AdlTaxiIsOrder\` array(nullable(bool)),
  \`item.AdlTaxiCurrentFee\` array(nullable(decimal(20, 4))),
  \`item.AdlParkingBeginTime\` array(nullable(datetime64(3))),
  \`item.AdlParkingEndTime\` array(nullable(datetime64(3))),
  \`item.AdlByerInfoEmail\` array(nullable(string)),
  \`item.AdlByerPhoneInfoNumber\` array(nullable(string)),
  \`item.AdlByerInfoNumber\` array(nullable(string)),
  \`item.FullSummNDS\` array(decimal(20, 4)),
  \`item.FiscalSign\` array(string),
  \`item.AutonomousSign\` array(string),
  \`item.Buyer_IIN_BIN\` array(nullable(string)),
  \`item.KkmUseAddress\` array(nullable(string)),
  \`item.DateTimeToOFD\` array(datetime64(3)),
  \`item.ProtocolVersion\` array(string),
  \`item.ElementId\` array(string)
) Engine = Memory;

INSERT INTO stateless_99073_complex_avro_schema VALUES ( ['txn-12345'], ['ABC123456'], ['Main Street 1'], ['Branch Office 42'], ['kkm-007'], [1], ['REG-001'], ['FACT-2023'], ['DOMAIN_TRADING'], ['OPERATION_BUY'], [1716940800123], ['cashier-01'], [123.4567], [100.0000], [5.5000], [2.0000], [1.2500], [['PAYMENT_CARD', 'PAYMENT_CASH']], [[50.0000, 50.0000]], [['ITEM_TYPE_COMMODITY']], [['prod-001']], [['Laptop']], [['sec-01']], [[1]], [[899.9900]], [[899.9900]], [[149.9983]], [['Summer Discount']], [[50.0000]], [[8.3333]], [['Returned Item']], [['sec-02']], [[1]], [[100.0000]], [[100.0000]], [[16.6667]], [['Discount Reversal']], [[10.0000]], [[1.6667]], [['MARK123']], [['STAMP-001']], [['pid-001']], [['GTIN0001']], [['kg']], ['ACC-98765'], ['CORR-001'], [1.2300], ['CARD-555'], ['KAZ123A'], [1], [500.0000], [1716940800000], [1716944400000], ['buyer@example.com'], ['+77771234567'], ['BYER-001'], [149.9983], ['FISCAL-SIG-1'], ['AUTO-SIG-1'], ['BUYER-IIN-123'], ['Shop Address 42'], [1716940800500], ['1.0'], ['elem-001']);
"

$CLICKHOUSE_CLIENT -q "SELECT * FROM stateless_99073_complex_avro_schema SETTINGS format_schema='$SCHEMASDIR/99073_complex_avro_schema.avsc' FORMAT Avro;" | md5sum -b

$CLICKHOUSE_CLIENT -q 'DROP STREAM stateless_99073_complex_avro_schema'
