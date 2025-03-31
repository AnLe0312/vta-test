CREATE TABLE IF NOT EXISTS prod_source.retailtendertypetable (
   TENDERTYPEID	String,
   NAME	String,
   DEFAULTFUNCTION	Int64,
   FISCALPRINTERTENDERTYPE_BR	String,
   MODIFIEDDATETIME	DateTime,
   DEL_MODIFIEDTIME	Int64,
   MODIFIEDBY	String,
   CREATEDDATETIME	DateTime,
   CREATEDBY	String,
   RECVERSION	Int64,
   PARTITION	UInt64,
   RECID	UInt64,
   last_synced_at               DateTime,
   updated_at    DateTime DEFAULT now(),
   PRIMARY KEY (RECID)
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (RECID, CREATEDDATETIME)
PARTITION BY toYYYYMM(CREATEDDATETIME)
TTL CREATEDDATETIME + INTERVAL 5 YEAR DELETE
SETTINGS index_granularity = 8192