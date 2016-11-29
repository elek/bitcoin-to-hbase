Simple script to import bitcoin blocks to hbase cluster.

Using reader from https://github.com/ZuInnoTe/hadoopcryptoledger

Using batched put requests (with disabled WAL) instead of Bulk import.

Build: 

```
gradle uberJar
```

