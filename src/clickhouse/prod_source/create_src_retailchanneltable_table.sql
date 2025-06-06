CREATE TABLE IF NOT EXISTS prod_source.retailchanneltable (
   MCRCUSTOMERCREDITRETAILINFOCODEID	String,
   MCRENABLEDIRECTEDSELLING	Int64,
   MCRENABLEORDERCOMPLETION	Int64,
   MCRENABLEORDERPRICECONTROL	Int64,
   MCRPRICEOVERRIDERETAILINFOCODEID	String,
   MCRREASONCODERETAILINFOCODEID	String,
   ONLINECATALOGNAME	String,
   STORENUMBER	String,
   PHONE	String,
   OPENFROM	Int64,
   OPENTO	Int64,
   STATEMENTMETHOD	Int64,
   ONESTATEMENTPERDAY	Int64,
   CLOSINGMETHOD	Int64,
   MAXIMUMPOSTINGDIFFERENCE	Float64,
   MAXROUNDINGAMOUNT	Float64,
   MAXSHIFTDIFFERENCEAMOUNT	Float64,
   MAXTRANSACTIONDIFFERENCEAMOUNT	Float64,
   FUNCTIONALITYPROFILE	String,
   CREATELABELSFORZEROPRICE	Int64,
   INVENTORYLOOKUP	Int64,
   REMOVEADDTENDER	String,
   TENDERDECLARATIONCALCULATION	Int64,
   MAXIMUMTEXTLENGTHONRECEIPT	Int64,
   NUMBEROFTOPORBOTTOMLINES	Int64,
   ITEMIDONRECEIPT	Int64,
   SERVICECHARGEPCT	Float64,
   SERVICECHARGEPROMPT	String,
   TAXGROUP	String,
   REPLICATIONCOUNTER	Int64,
   ROUNDINGTAXACCOUNT	String,
   MAXROUNDINGTAXAMOUNT	Float64,
   USEDEFAULTCUSTACCOUNT	Int64,
   GENERATESSHELFLABELS	Int64,
   GENERATESITEMLABELS	Int64,
   CULTURENAME	String,
   RETAILREQPLANIDSCHED	String,
   SQLSERVERNAME	String,
   DATABASENAME	String,
   USERNAME	String,
   PASSWORD	String,
   POITEMFILTER	Int64,
   HIDETRAININGMODE	Int64,
   STMTCALCBATCHENDTIME	Int64,
   SEPARATESTMTPERSTAFFTERMINAL	Int64,
   USEDESTINATIONBASEDTAX	Int64,
   TAXIDENTIFICATIONNUMBER	String,
   USECUSTOMERBASEDTAX	Int64,
   INVENTLOCATIONIDFORCUSTOMERORDER	String,
   OFFLINEPROFILE	UInt64,
   RETURNTAXGROUP_W	String,
   ROUNDINGACCOUNTLEDGERDIMENSION	UInt64,
   STMTPOSTASBUSINESSDAY	Int64,
   TAXGROUPDATAAREAID	String,
   TAXOVERRIDEGROUP	UInt64,
   EFTSTORENUMBER	String,
   FISCALAUDITINGREQUIRED	Int64,
   LINKEDEFDOCUMENTTAXGROUP	String,
   CASHOFFICE_RU	String,
   CATEGORYHIERARCHY	UInt64,
   CHANNELTIMEZONE	UInt64,
   CHANNELTIMEZONEINFOID	String,
   CHANNELTYPE	Int64,
   CURRENCY	String,
   DEFAULTCUSTACCOUNT	String,
   DEFAULTCUSTDATAAREAID	String,
   DEFAULTDIMENSION	UInt64,
   EVENTNOTIFICATIONPROFILEID	String,
   INSTANCERELATIONTYPE	UInt64,
   INVENTLOCATION	String,
   INVENTLOCATIONDATAAREAID	String,
   OMOPERATINGUNITID	UInt64,
   PAYMENT	String,
   PAYMMODE	String,
   PRICEINCLUDESSALESTAX	Int64,
   STOREAREA	Float64,
   TRANSACTIONSERVICEPROFILE	String,
   DISPLAYTAXPERTAXCOMPONENT	Int64,
   EFDOCTRANSACTIONSERVICEPROFILE	String,
   RECVERSION	Int64,
   RELATIONTYPE	UInt64,
   PARTITION	UInt64,
   RECID	UInt64,
   MODIFIEDDATETIME	DateTime,
   MODIFIEDBY	String,
   CREATEDDATETIME	DateTime,
   CREATEDBY	String,
   VTVRETAILSUMLOGCALCMETHOD	Int64,
   PERCENT_	Float64,
   VTASTOREACTIVE	Int64,
   VTASTOREAREAID	String,
   VTASTORECLASSIFICATION	Int64,
   VTACLOSINGDATE	Nullable(DateTime),
   VTASTOREAREAEFFECTIVEDATE	Nullable(DateTime),
   VTASTORESIZE	Int64,
   VTAAREASALESMANAGER	String,
   VTASTOREMANAGER	String,
   last_synced_at               DateTime,
   updated_at    DateTime DEFAULT now(),
   PRIMARY KEY (RECID)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (RECID, CREATEDDATETIME)
PARTITION BY toYYYYMM(CREATEDDATETIME)
TTL CREATEDDATETIME + INTERVAL 5 YEAR DELETE
SETTINGS index_granularity = 8192