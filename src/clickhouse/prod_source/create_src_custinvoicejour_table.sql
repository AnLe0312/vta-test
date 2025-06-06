CREATE TABLE IF NOT EXISTS prod_source.custinvoicejour (
    CUSTGROUP                   String,
    REFNUM                      Int64,                   
    SALESID                     String,
    ORDERACCOUNT                String,
    INVOICEACCOUNT              String,
    INVOICEDATE                 DateTime,
    DUEDATE                     DateTime,
    CASHDISC                    Float64,
    CASHDISCDATE                Nullable(DateTime),
    QTY                         Int64,
    VOLUME                      Float64,
    WEIGHT                      Int64,
    SUMLINEDISC                 Float64,
    SALESBALANCE                Float64,
    ENDDISC                     Float32,
    INVOICEAMOUNT               Float64,
    CURRENCYCODE                String,
    EXCHRATE                    Float32,
    INVOICEID                   String,
    LEDGERVOUCHER               String,
    UPDATED                     Int64,
    ONACCOUNTAMOUNT             Float64,
    TAXPRINTONINVOICE           Int64,
    LISTCODE                    Int64,
    DOCUMENTNUM                 String,
    DOCUMENTDATE                Nullable(DateTime),
    CASHDISCPERCENT             Float32,
    INTRASTATDISPATCH           String,
    DELIVERYNAME                String,
    ENTERPRISENUMBER            String,
    PURCHASEORDER               String,
    DLVTERM                     String,
    DLVMODE                     String,
    PAYMENT                     String,
    CASHDISCCODE                String,
    INVOICEROUNDOFF             Decimal(32,16),
    SUMMARKUP                   Decimal(32,16),
    COVSTATUS                   UInt8,
    RETURNITEMNUM               String,
    POSTINGPROFILE              String,
    BACKORDER                   Int64,
    PREPAYMENT                  Int64,
    TAXGROUP                    String,
    TAXITEMGROUP                String,
    TAXSPECIFYBYLINE            Int64,
    EINVOICELINESPECIFIC        Int64,
    ONETIMECUSTOMER             Int64,
    PAYMENTSCHED                String,
    SUMTAX                      Float64,
    SALESTYPE                   UInt8,
    EINVOICEACCOUNTCODE         String,
    INTERCOMPANYPOSTED          Int64,
    PARMID                      String,
    RETURNREASONCODEID          String,
    EUSALESLIST                 String,
    EXCHRATESECONDARY           Decimal(32,16),
    TRIANGULATION               Int64,
    CUSTOMERREF                 String,
    VATNUM                      String,
    NUMBERSEQUENCEGROUP         String,
    LANGUAGEID                  String,
    INCLTAX                     UInt8,
    LOG                         String,
    PAYMDAYID                   String,
    INVOICINGNAME               String,
    GIROTYPE                    UInt8,
    CONTACTPERSONID             String,
    SALESORIGINID               String,
    BILLOFLADINGID              String,
    INVENTLOCATIONID            String,
    FIXEDDUEDATE                Nullable(DateTime),
    INVOICEAMOUNTMST            Float64,
    INVOICEROUNDOFFMST          Float64,
    SUMMARKUPMST                Float64,
    SUMLINEDISCMST              Float64,
    ENDDISCMST                  Float64,
    SALESBALANCEMST             Float64,
    SUMTAXMST                   Float64,
    PRINTMGMTSITEID             String,
    RETURNSTATUS                UInt8,
    INTERCOMPANYCOMPANYID       String,
    INTERCOMPANYPURCHID         String,
    PRINTEDORIGINALS            Int64,
    PROFORMA                    Int64,
    RCSALESLIST_UK              String,
    REVERSECHARGE_UK            Decimal(32,16),
    DELIVERYPOSTALADDRESS       UInt64,
    INVOICEPOSTALADDRESS        UInt64,
    SOURCEDOCUMENTHEADER        UInt64,
    DEFAULTDIMENSION            UInt64,
    BANKLCEXPORTLINE            UInt64,
    WORKERSALESTAKER            UInt64,
    REVERSEDRECID               UInt64,
    RECEIPTDATECONFIRMED_ES     Nullable(DateTime),
    PAYMID                      String,
    TAXINVOICESALESID           String,
    INTRASTATFULFILLMENTDATE_HU Nullable(DateTime),
    MCREMAIL                    String,
    MCRPAYMAMOUNT               Float64,
    MCRDUEAMOUNT                Float64,
    CASHDISCBASEDATE            Nullable(DateTime),
    DIRECTDEBITMANDATE          UInt64,
    ISCORRECTION                Int64,
    REASONTABLEREF              UInt64,
    SOURCEDOCUMENTLINE          UInt64,
    TRANSPORTATIONDOCUMENT      UInt64,
    VASCOMBINATION              Int64,
    VASFORMFORMAT               String,
    VASFORMNUM                  String,
    VASINVOICEDATE              Nullable(DateTime),
    VASINVOICENUM               String,
    VASISRETURNINVOICE          UInt64,
    VASORIGINALINVOICE          UInt64,
    VASPURCHASERNAME            String,
    VASRECIDGROUP               UInt64,
    VASSERIALNUM                String,
    VASTAXCOMPANYNAME           String,
    VASTAXREGNUM                String,
    VASTAXTRANSTXT              String,
    VASVATADDRESS               String,
    MODIFIEDDATETIME            DateTime,
    CREATEDDATETIME             DateTime,
    DEL_CREATEDTIME             UInt64,
    CREATEDBY                   String,
    DATAAREAID                  String,
    RECVERSION                  UInt64,
    PARTITION                   UInt64,
    RECID                       UInt64,
    CANCELLATION                Int64,
    VASCANCELORGINVOICE         Int64,
    VASCUSTREQUEST              Int64,
    VASEINVOICEEMAIL            String,
    VASINVOICEADDRESS           String,
    VASLINEAMOUNT               Float64,
    VASLOGKEY                   String,
    VASREPLACEORGINVOICE        Int64,
    VASUSEEINVOICE              Int64,
    VASORGEINVOICEDATA          Int64,
    VTAINVNOBYCUST              Int64,
    VASADJUSTREDUCEQTY          Int64,
    VASINVOICEREASON            String,
    VASVATDISCCOUNT             Float64,
    VTA_SALESTYPE               UInt8,
    YVS_EINVOICEFROM            Int64,
    last_synced_at              DateTime,
    updated_at                  DateTime DEFAULT now(),
    PRIMARY KEY (RECID)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (RECID, INVOICEDATE)
PARTITION BY toYYYYMM(INVOICEDATE)
TTL INVOICEDATE + INTERVAL 5 YEAR DELETE
SETTINGS index_granularity = 8192