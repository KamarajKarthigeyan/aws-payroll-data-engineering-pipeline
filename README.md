This module implements a production-grade multi-table join ETL pipeline with comprehensive data lineage tracking. The implementation demonstrates joining sales and region data, with full lineage visibility from source tables through transformations to the final output.
Bucket: dz-demo-mt
Region: us-east-1 (with replication to us-east-2)

**Key Features:**
Data Lineage Tracking: Complete visibility from raw sources through transformations to final output.
Automated Schema Discovery: Crawlers maintain up-to-date metadata.
Partitioned Storage: Optimized for time-based queries.
Geographic Redundancy: Cross-region replication for resilience.
Multi-Table Join: Combines sales and regional data for enriched analytics.
