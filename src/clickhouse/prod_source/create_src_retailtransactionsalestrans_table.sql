CREATE TABLE IF NOT EXISTS prod_source.retailtransactionsalestrans (
   CURRENCY	String,
   TRANSACTIONID	String,
   LINENUM	UInt32,
   RECEIPTID	String,
   BARCODE	String,
   ITEMID	String,
   PRICE	Float64,
   NETPRICE	Float64,
   QTY	Int64,
   TAXGROUP	String,
   TRANSACTIONSTATUS	UInt16,
   DISCAMOUNT	Float64,
   COSTAMOUNT	Float64,
   TRANSDATE	DateTime,
   TRANSTIME	UInt32,
   SHIFT	String,
   SHIFTDATE	Nullable(DateTime),
   NETAMOUNT	Float64,
   TAXAMOUNT	Float64,
   DISCOFFERID	String,
   STDNETPRICE	Float64,
   DISCAMOUNTFROMSTDPRICE	Float64,
   STATEMENTID	String,
   CUSTACCOUNT	String,
   SECTION	String,
   SHELF	String,
   STATEMENTCODE	String,
   DISCGROUPID	String,
   TRANSACTIONCODE	Int64,
   STORE	String,
   ITEMIDSCANNED	Int64,
   KEYBOARDITEMENTRY	Int64,
   PRICEINBARCODE	Int64,
   PRICECHANGE	Int64,
   WEIGHTMANUALLYENTERED	Int64,
   LINEWASDISCOUNTED	Int64,
   SCALEITEM	Int64,
   WEIGHTITEM	Int64,
   RETURNNOSALE	Int64,
   ITEMCORRECTEDLINE	Int64,
   LINKEDITEMNOTORIGINAL	Int64,
   ORIGINALOFLINKEDITEMLIST	Int64,
   TERMINALID	String,
   ITEMPOSTINGGROUP	String,
   TOTALROUNDEDAMOUNT	Float64,
   COUNTER	Int64,
   VARIANTID	String,
   LINEDSCAMOUNT	Float64,
   REPLICATED	Int64
   CUSTDISCAMOUNT	Float64,
   INFOCODEDISCAMOUNT	Float64,
   CUSTINVOICEDISCAMOUNT	Float64,
   UNIT	String,
   UNITQTY	Int64,
   UNITPRICE	Float64,
   TOTALDISCAMOUNT	Float64,
   TOTALDISCPCT	Float64,
   TOTALDISCINFOCODELINENUM	Float64,
   PERIODICDISCTYPE	Int64,
   PERIODICDISCAMOUNT	Float64,
   DISCOUNTAMOUNTFORPRINTING	Float64,
   STAFFID	String,
   PERIODICDISCGROUP	String,
   INVENTTRANSID	String,
   INVENTDIMID	String,
   PURCHID	String,
   FILELOGID	String,
   CONCESSIONCONTRACTID	String,
   CONCESSIONSETTLEMENTID	String,
   REPLICATIONCOUNTERFROMORIGIN	Int64
   CONSESSIONPARTPAYMENTID	String,
   PRESCRIPTIONID	String,
   COMMENT_	String,
   PUMPID	Int64,
   INVENTSTATUSSALES	UInt32,
   INVENTBATCHID	String,
   GIFTCARD	Int64,
   RFIDTAGID	String,
   INVENTSERIALID	String,
   RETURNTRANSACTIONID	String,
   RETURNQTY	Int64,
   TAXITEMGROUP	String,
   ORIGINALTAXGROUP	String,
   ORIGINALTAXITEMGROUP	String,
   NETAMOUNTINCLTAX	Float64,
   BLOCKQTY	Float64,
   BUSINESSDATE	Nullable(DateTime),
   CATALOG	UInt64,
   CATEGORYID	UInt64,
   CHANNEL	UInt64,
   DEFAULTDIMENSION	UInt64,
   DLVMODE	String,
   ELECTRONICDELIVERYEMAIL	String,
   ELECTRONICDELIVERYEMAILCONTENT	String,
   INVENTLOCATIONID	String,
   INVENTSITEID	String,
   LINEMANUALDISCOUNTAMOUNT	Float64,
   LINEMANUALDISCOUNTPERCENTAGE	Float64,
   LISTINGID	String,
   LOGISTICSPOSTALADDRESS	UInt64,
   LOYALTYDISCAMOUNT_RU	Float64,
   LOYALTYDISCPCT_RU	Float64,
   ORIGIN	String,
   ORIGINALPRICE	Float64,
   PERIODICPERCENTAGEDISCOUNT	Float64,
   RECEIPTDATEREQUESTED	Nullable(DateTime),
   RETURNLINENUM	Int64,
   RETURNSTORE	String,
   RETURNTERMINALID	String,
   SHIPPINGDATEREQUESTED	Nullable(DateTime),
   SKIPSALESLINE_RU	Int64,
   CUSTACCOUNTASYNC	String,
   MODIFIEDDATETIME	DateTime,
   DEL_MODIFIEDTIME	Int64,
   MODIFIEDBY	String,
   MODIFIEDTRANSACTIONID	UInt64,
   CREATEDDATETIME	DateTime,
   DEL_CREATEDTIME	Int64,
   CREATEDBY	String,
   CREATEDTRANSACTIONID	UInt64,
   DATAAREAID	String,
   RECVERSION	Int64,
   PARTITION	UInt64,
   RECID	UInt64,
   VRTGIFTCARDNUMBER	String,
   STAFF	String,
   updated_at    DateTime DEFAULT now(),
   PRIMARY KEY (RECID)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (RECID, TRANSDATE)
PARTITION BY toYYYYMM(TRANSDATE)
TTL TRANSDATE + INTERVAL 5 YEAR DELETE
SETTINGS index_granularity = 8192