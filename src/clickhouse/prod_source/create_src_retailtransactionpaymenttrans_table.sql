CREATE TABLE IF NOT EXISTS prod_source.retailtransactionpaymenttrans (
   TRANSACTIONID	String,
   LINENUM	UInt32,
   RECEIPTID	String,
   STATEMENTCODE	String,
   CARDTYPEID	String,
   EXCHRATE	Float64,
   TENDERTYPE	String,
   AMOUNTTENDERED	Float64,
   CURRENCY	String,
   AMOUNTCUR	Float64,
   CARDORACCOUNT	String,
   TRANSDATE	DateTime,
   TRANSTIME	UInt32,
   SHIFT	String,
   SHIFTDATE	Nullable(DateTime),
   STAFF	String,
   STORE	String,
   TERMINAL	String,
   TRANSACTIONSTATUS	UInt32,
   STATEMENTID	String,
   MANAGERKEYLIVE	Int64,
   CHANGELINE	Int64,
   COUNTER	Int64,
   MESSAGENUM	Int64,
   REPLICATED	Int64,
   QTY	Int64,
   REPLICATIONCOUNTERFROMORIGIN	Int64,
   AUTHENTICATIONCODE	String,
   GIFTCARDID	String,
   CREDITVOUCHERID	String,
   LOYALTYCARDID	String,
   AMOUNTMST	Float64,
   EXCHRATEMST	Float64,
   BUSINESSDATE	Nullable(DateTime),
   CASHDOCID_RU	String,
   CHANNEL	UInt64,
   DEFAULTDIMENSION	UInt64,
   ISPREPAYMENT	Int64,
   ORIGIN	String,
   PAYMENTAUTHORIZATION	String,
   SIGCAPDATA	String,
   CARDBRANDCODE	String,
   CARDBRANDNAME	String,
   CARDINSTALLMENTS	Int64,
   ISCAPTUREFAILED	Int64,
   PAYMENTCARDTOKEN	String,
   MODIFIEDDATETIME	DateTime,
   DEL_MODIFIEDTIME	Int64,
   MODIFIEDBY	String,
   CREATEDDATETIME	DateTime,
   DEL_CREATEDTIME	Int64,
   CREATEDBY	String,
   DATAAREAID	String,
   RECVERSION	Int64,
   PARTITION	UInt64,
   RECID	UInt64,
   last_synced_at    DateTime,
   updated_at    DateTime DEFAULT now(),
   PRIMARY KEY (RECID)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (RECID, TRANSDATE)
PARTITION BY toYYYYMM(TRANSDATE)
TTL TRANSDATE + INTERVAL 5 YEAR DELETE
SETTINGS index_granularity = 8192