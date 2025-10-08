CREATE DATABASE ReusablePackagingDB;

USE ReusablePackagingDB;

/* ======================================================================
   2) Core reference — places & actors
   ====================================================================== */

-- Locations: retailers, hubs (washing), or dropboxes
CREATE TABLE dbo.locations (
  location_id   BIGINT          NOT NULL IDENTITY(1,1),
  name          NVARCHAR(120)   NOT NULL,
  kind          NVARCHAR(20)    NOT NULL,  -- 'retailer','hub','dropbox'
  address       NVARCHAR(255)       NULL,
  lat           DECIMAL(9,6)        NULL,
  lng           DECIMAL(9,6)        NULL,
  CONSTRAINT pk_locations PRIMARY KEY (location_id),
  CONSTRAINT uq_locations_name UNIQUE (name),
  CONSTRAINT ck_locations_kind CHECK (kind IN (N'retailer',N'hub',N'dropbox'))
);

-- Retailers (one-to-one with a location of kind='retailer')
CREATE TABLE dbo.retailers (
  retailer_id   BIGINT          NOT NULL IDENTITY(1,1),
  location_id   BIGINT          NOT NULL,
  contact_email NVARCHAR(190)       NULL,
  CONSTRAINT pk_retailers PRIMARY KEY (retailer_id),
  CONSTRAINT uq_retailers_location UNIQUE (location_id),
  CONSTRAINT fk_retailers_location
    FOREIGN KEY (location_id) REFERENCES dbo.locations(location_id)
    ON DELETE NO ACTION
);

-- Hubs (washing facilities) — one-to-one with a location of kind='hub'
CREATE TABLE dbo.hubs (
  hub_id        BIGINT          NOT NULL IDENTITY(1,1),
  location_id   BIGINT          NOT NULL,
  washer_model  NVARCHAR(80)        NULL,
  CONSTRAINT pk_hubs PRIMARY KEY (hub_id),
  CONSTRAINT uq_hubs_location UNIQUE (location_id),
  CONSTRAINT fk_hubs_location
    FOREIGN KEY (location_id) REFERENCES dbo.locations(location_id)
    ON DELETE NO ACTION
);

-- Customers
CREATE TABLE dbo.customers (
  customer_id   BIGINT          NOT NULL IDENTITY(1,1),
  name          NVARCHAR(120)   NOT NULL,
  email         NVARCHAR(190)       NULL UNIQUE,
  phone         NVARCHAR(40)        NULL,
  created_at    DATETIME2       NOT NULL CONSTRAINT df_customers_created DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_customers PRIMARY KEY (customer_id)
);

CREATE INDEX idx_locations_kind ON dbo.locations(kind);

/* ======================================================================
   3) Catalog & instances
   ====================================================================== */

-- Packaging catalog (SKU/kind/material determines deposit)
CREATE TABLE dbo.packaging_catalog (
  catalog_id           BIGINT        NOT NULL IDENTITY(1,1),
  sku                  NVARCHAR(64)      NULL UNIQUE,
  kind                 NVARCHAR(20)  NOT NULL,    -- 'cup','box','jar'
  material             NVARCHAR(30)  NOT NULL,    -- 'pp','stainless','glass'
  capacity_ml          INT               NULL,
  deposit_amount_cents INT           NOT NULL,
  CONSTRAINT pk_packaging_catalog PRIMARY KEY (catalog_id),
  CONSTRAINT ck_catalog_kind    CHECK (kind IN (N'cup',N'box',N'jar')),
  CONSTRAINT ck_catalog_deposit CHECK (deposit_amount_cents >= 0)
);

-- Physical instances (each has a unique UID: QR/RFID)
CREATE TABLE dbo.packaging_instances (
  instance_id    BIGINT         NOT NULL IDENTITY(1,1),
  catalog_id     BIGINT         NOT NULL,
  uid_code       NVARCHAR(64)   NOT NULL,
  state          NVARCHAR(20)   NOT NULL CONSTRAINT df_instances_state DEFAULT N'available',
  birthed_at     DATETIME2      NOT NULL CONSTRAINT df_instances_birthed DEFAULT SYSUTCDATETIME(),
  retired_at     DATETIME2          NULL,
  CONSTRAINT pk_packaging_instances PRIMARY KEY (instance_id),
  CONSTRAINT uq_instances_uid UNIQUE (uid_code),
  CONSTRAINT fk_instances_catalog
    FOREIGN KEY (catalog_id) REFERENCES dbo.packaging_catalog(catalog_id)
    ON DELETE NO ACTION,
  CONSTRAINT ck_instances_state CHECK (state IN
    (N'available',N'in_use',N'at_retailer',N'at_hub',N'washing',N'damaged',N'lost',N'retired'))
);

CREATE INDEX idx_instances_catalog ON dbo.packaging_instances(catalog_id);
CREATE INDEX idx_instances_state   ON dbo.packaging_instances(state);

/* ======================================================================
   4) Deposits (accounts & ledger)
   ====================================================================== */

-- One deposit account per customer
CREATE TABLE dbo.deposit_accounts (
  account_id    BIGINT  NOT NULL IDENTITY(1,1),
  customer_id   BIGINT  NOT NULL,
  balance_cents INT     NOT NULL CONSTRAINT df_accounts_balance DEFAULT (0),
  CONSTRAINT pk_deposit_accounts PRIMARY KEY (account_id),
  CONSTRAINT uq_deposit_customer UNIQUE (customer_id),
  CONSTRAINT fk_accounts_customer
    FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id)
    ON DELETE CASCADE
);

-- Ledger of deposit movements
CREATE TABLE dbo.deposit_transactions (
  tx_id        BIGINT        NOT NULL IDENTITY(1,1),
  account_id   BIGINT        NOT NULL,
  delta_cents  INT           NOT NULL,          -- +credit / -debit
  reason       NVARCHAR(120) NOT NULL,          -- 'checkout_hold','return_release','penalty','adjustment'
  ref_table    NVARCHAR(40)      NULL,          -- optional pointer
  ref_id       BIGINT            NULL,
  created_at   DATETIME2     NOT NULL CONSTRAINT df_tx_created DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_deposit_transactions PRIMARY KEY (tx_id),
  CONSTRAINT fk_tx_account
    FOREIGN KEY (account_id) REFERENCES dbo.deposit_accounts(account_id)
    ON DELETE CASCADE
);

CREATE INDEX idx_tx_account_time ON dbo.deposit_transactions(account_id, created_at);

/* ======================================================================
   5) Flows: checkouts / returns / wash cycles / inspections / incidents
   ====================================================================== */

-- Checkout: retailer → customer
CREATE TABLE dbo.checkouts (
  checkout_id    BIGINT    NOT NULL IDENTITY(1,1),
  instance_id    BIGINT    NOT NULL,
  retailer_id    BIGINT    NOT NULL,
  customer_id    BIGINT    NOT NULL,
  checkout_time  DATETIME2 NOT NULL CONSTRAINT df_co_time DEFAULT SYSUTCDATETIME(),
  due_back_days  INT       NOT NULL CONSTRAINT df_co_due DEFAULT (7),
  CONSTRAINT pk_checkouts PRIMARY KEY (checkout_id),
  CONSTRAINT fk_co_instance
    FOREIGN KEY (instance_id) REFERENCES dbo.packaging_instances(instance_id)
    ON DELETE NO ACTION,
  CONSTRAINT fk_co_retailer
    FOREIGN KEY (retailer_id) REFERENCES dbo.retailers(retailer_id)
    ON DELETE NO ACTION,
  CONSTRAINT fk_co_customer
    FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id)
    ON DELETE NO ACTION
);

CREATE INDEX idx_co_instance        ON dbo.checkouts(instance_id);
CREATE INDEX idx_co_customer_time   ON dbo.checkouts(customer_id, checkout_time);

-- Return: customer → retailer or dropbox (may be anonymous)
CREATE TABLE dbo.returns (
  return_id     BIGINT    NOT NULL IDENTITY(1,1),
  instance_id   BIGINT    NOT NULL,
  customer_id   BIGINT        NULL,    -- allow NULL for anonymous/dropbox
  location_id   BIGINT    NOT NULL,    -- retailer or dropbox
  return_time   DATETIME2  NOT NULL CONSTRAINT df_ret_time DEFAULT SYSUTCDATETIME(),
  checkout_id   BIGINT        NULL,    -- link back when matched
  CONSTRAINT pk_returns PRIMARY KEY (return_id),
  CONSTRAINT fk_ret_instance
    FOREIGN KEY (instance_id) REFERENCES dbo.packaging_instances(instance_id)
    ON DELETE NO ACTION,
  CONSTRAINT fk_ret_customer
    FOREIGN KEY (customer_id) REFERENCES dbo.customers(customer_id)
    ON DELETE SET NULL,
  CONSTRAINT fk_ret_location
    FOREIGN KEY (location_id) REFERENCES dbo.locations(location_id)
    ON DELETE NO ACTION,
  CONSTRAINT fk_ret_checkout
    FOREIGN KEY (checkout_id) REFERENCES dbo.checkouts(checkout_id)
    ON DELETE SET NULL
);

CREATE INDEX idx_ret_instance_time ON dbo.returns(instance_id, return_time);
CREATE INDEX idx_ret_location_time ON dbo.returns(location_id, return_time);

-- Wash cycles at hubs
CREATE TABLE dbo.wash_cycles (
  wash_id     BIGINT       NOT NULL IDENTITY(1,1),
  hub_id      BIGINT       NOT NULL,
  batch_code  NVARCHAR(40) NOT NULL,
  start_time  DATETIME2    NOT NULL CONSTRAINT df_wash_start DEFAULT SYSUTCDATETIME(),
  end_time    DATETIME2        NULL,
  temp_c      DECIMAL(5,2)     NULL,
  detergent   NVARCHAR(40)     NULL,
  CONSTRAINT pk_wash PRIMARY KEY (wash_id),
  CONSTRAINT fk_wash_hub
    FOREIGN KEY (hub_id) REFERENCES dbo.hubs(hub_id)
    ON DELETE NO ACTION
);

CREATE INDEX idx_wash_hub_time ON dbo.wash_cycles(hub_id, start_time);

-- Post-wash inspections
CREATE TABLE dbo.inspections (
  inspection_id BIGINT       NOT NULL IDENTITY(1,1),
  instance_id   BIGINT       NOT NULL,
  wash_id       BIGINT           NULL,
  inspector     NVARCHAR(80)     NULL,
  result        NVARCHAR(12) NOT NULL,  -- 'pass','fail'
  notes         NVARCHAR(255)    NULL,
  inspected_at  DATETIME2    NOT NULL CONSTRAINT df_insp_time DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_inspections PRIMARY KEY (inspection_id),
  CONSTRAINT fk_insp_instance
    FOREIGN KEY (instance_id) REFERENCES dbo.packaging_instances(instance_id)
    ON DELETE NO ACTION,
  CONSTRAINT fk_insp_wash
    FOREIGN KEY (wash_id) REFERENCES dbo.wash_cycles(wash_id)
    ON DELETE SET NULL,
  CONSTRAINT ck_insp_result CHECK (result IN (N'pass',N'fail'))
);

CREATE INDEX idx_insp_instance_time ON dbo.inspections(instance_id, inspected_at);

-- Contamination incidents
CREATE TABLE dbo.contamination_incidents (
  incident_id  BIGINT       NOT NULL IDENTITY(1,1),
  instance_id  BIGINT       NOT NULL,
  kind         NVARCHAR(30) NOT NULL,  -- 'microbial','chemical','foreign_matter'
  severity     TINYINT      NOT NULL,  -- 1..5
  description  NVARCHAR(255)    NULL,
  detected_at  DATETIME2    NOT NULL CONSTRAINT df_contam_time DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_incidents PRIMARY KEY (incident_id),
  CONSTRAINT fk_contam_instance
    FOREIGN KEY (instance_id) REFERENCES dbo.packaging_instances(instance_id)
    ON DELETE NO ACTION,
  CONSTRAINT ck_contam_kind CHECK (kind IN (N'microbial',N'chemical',N'foreign_matter')),
  CONSTRAINT ck_contam_severity CHECK (severity BETWEEN 1 AND 5)
);

CREATE INDEX idx_contam_instance_time ON dbo.contamination_incidents(instance_id, detected_at);


/* ======================================================================
   6) Telemetry & movement tracking
   ====================================================================== */

-- Sensor readings attached to an instance and/or a location.
-- Use SET NULL to avoid cascading chains to the same child.
CREATE TABLE dbo.sensor_readings (
  reading_id   BIGINT        NOT NULL IDENTITY(1,1),
  instance_id  BIGINT            NULL,
  location_id  BIGINT            NULL,
  sensor_type  NVARCHAR(20)  NOT NULL,  -- 'temperature','shock','humidity'
  value        DECIMAL(10,3)  NOT NULL,
  measured_at  DATETIME2      NOT NULL CONSTRAINT df_sr_time DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_sensor_readings PRIMARY KEY (reading_id),
  CONSTRAINT fk_sr_instance
    FOREIGN KEY (instance_id) REFERENCES dbo.packaging_instances(instance_id)
    ON DELETE SET NULL,
  CONSTRAINT fk_sr_location
    FOREIGN KEY (location_id) REFERENCES dbo.locations(location_id)
    ON DELETE SET NULL,
  CONSTRAINT ck_sr_type CHECK (sensor_type IN (N'temperature',N'shock',N'humidity'))
);



CREATE INDEX idx_sr_instance_time ON dbo.sensor_readings(instance_id, measured_at);
CREATE INDEX idx_sr_location_time ON dbo.sensor_readings(location_id, measured_at);

-- Movements: scans recording last known transitions
CREATE TABLE dbo.movements (
  mv_id        BIGINT       NOT NULL IDENTITY(1,1),
  instance_id  BIGINT       NOT NULL,
  from_loc_id  BIGINT           NULL,
  to_loc_id    BIGINT           NULL,
  moved_at     DATETIME2    NOT NULL CONSTRAINT df_mv_time DEFAULT SYSUTCDATETIME(),
  note         NVARCHAR(120)    NULL,
  CONSTRAINT pk_movements PRIMARY KEY (mv_id),
  CONSTRAINT fk_mv_instance
    FOREIGN KEY (instance_id) REFERENCES dbo.packaging_instances(instance_id)
    ON DELETE NO ACTION,
  CONSTRAINT fk_mv_from
    FOREIGN KEY (from_loc_id) REFERENCES dbo.locations(location_id)
    ON DELETE NO ACTION,
  CONSTRAINT fk_mv_to
    FOREIGN KEY (to_loc_id)   REFERENCES dbo.locations(location_id)
    ON DELETE SET NULL
);

CREATE INDEX idx_mv_instance_time ON dbo.movements(instance_id, moved_at);
CREATE INDEX idx_mv_to_time       ON dbo.movements(to_loc_id, moved_at);

select * from dbo.movements;

CREATE INDEX idx_mv_instance_time ON dbo.movements(instance_id, moved_at);
CREATE INDEX idx_mv_to_time       ON dbo.movements(to_loc_id, moved_at);

/* ======================================================================
   7) Audit (lightweight, no FKs for flexibility)
   ====================================================================== */
CREATE TABLE dbo.audit_logs (
  log_id      BIGINT        NOT NULL IDENTITY(1,1),
  entity_type NVARCHAR(30)  NOT NULL,  -- 'instance','checkout','return','wash','inspection', etc.
  entity_id   BIGINT        NOT NULL,
  event_type  NVARCHAR(40)  NOT NULL,  -- 'STATE_CHANGE','PENALTY','ADJUST','NOTE'
  detail      NVARCHAR(MAX)     NULL,  -- optional JSON/text
  created_at  DATETIME2     NOT NULL CONSTRAINT df_audit_time DEFAULT SYSUTCDATETIME(),
  CONSTRAINT pk_audit_logs PRIMARY KEY (log_id)
);




-- INSERTS FOR dbo.locations (90 rows: 30 retailer, 30 hub, 30 dropbox)
SET IDENTITY_INSERT dbo.locations ON;
INSERT INTO dbo.locations (location_id, name, kind, address, lat, lng) VALUES
(1, N'Retailer Loc 01', N'retailer', N'Pine Avenue 243, Barcelona', 41.362068, 2.19764),
(2, N'Retailer Loc 02', N'retailer', N'Market Street 211, Barcelona', 41.392871, 2.154853),
(3, N'Retailer Loc 03', N'retailer', N'Central Ave 233, Barcelona', 41.390595, 2.105624),
(4, N'Retailer Loc 04', N'retailer', N'Sunset Blvd 108, Barcelona', 41.355588, 2.113607),
(5, N'Retailer Loc 05', N'retailer', N'Sunset Blvd 16, Barcelona', 41.416148, 2.11857),
(6, N'Retailer Loc 06', N'retailer', N'Oak Lane 162, Barcelona', 41.400195, 2.242156),
(7, N'Retailer Loc 07', N'retailer', N'King Street 150, Barcelona', 41.381734, 2.246438),
(8, N'Retailer Loc 08', N'retailer', N'Central Ave 143, Barcelona', 41.418677, 2.143441),
(9, N'Retailer Loc 09', N'retailer', N'River Road 139, Barcelona', 41.359423, 2.146272),
(10, N'Retailer Loc 10', N'retailer', N'River Road 27, Barcelona', 41.396528, 2.195837),
(11, N'Retailer Loc 11', N'retailer', N'Pine Avenue 25, Barcelona', 41.39382, 2.109418),
(12, N'Retailer Loc 12', N'retailer', N'Central Ave 159, Barcelona', 41.366477, 2.20206),
(13, N'Retailer Loc 13', N'retailer', N'Sunset Blvd 199, Barcelona', 41.375132, 2.187834),
(14, N'Retailer Loc 14', N'retailer', N'Hillcrest Rd 93, Barcelona', 41.373981, 2.219157),
(15, N'Retailer Loc 15', N'retailer', N'Oak Lane 21, Barcelona', 41.395954, 2.178779),
(16, N'Retailer Loc 16', N'retailer', N'Pine Avenue 187, Barcelona', 41.385907, 2.191344),
(17, N'Retailer Loc 17', N'retailer', N'Market Street 31, Barcelona', 41.390955, 2.124744),
(18, N'Retailer Loc 18', N'retailer', N'Pine Avenue 39, Barcelona', 41.424662, 2.163255),
(19, N'Retailer Loc 19', N'retailer', N'Market Street 196, Barcelona', 41.394646, 2.218364),
(20, N'Retailer Loc 20', N'retailer', N'Pine Avenue 88, Barcelona', 41.405624, 2.189155),
(21, N'Retailer Loc 21', N'retailer', N'King Street 205, Barcelona', 41.386496, 2.225995),
(22, N'Retailer Loc 22', N'retailer', N'Maple Street 122, Barcelona', 41.405763, 2.10975),
(23, N'Retailer Loc 23', N'retailer', N'Maple Street 166, Barcelona', 41.396236, 2.202186),
(24, N'Retailer Loc 24', N'retailer', N'Hillcrest Rd 73, Barcelona', 41.40733, 2.233056),
(25, N'Retailer Loc 25', N'retailer', N'Pine Avenue 6, Barcelona', 41.425252, 2.15332),
(26, N'Retailer Loc 26', N'retailer', N'King Street 30, Barcelona', 41.389495, 2.132731),
(27, N'Retailer Loc 27', N'retailer', N'Maple Street 34, Barcelona', 41.409069, 2.159685),
(28, N'Retailer Loc 28', N'retailer', N'Hillcrest Rd 21, Barcelona', 41.363309, 2.160247),
(29, N'Retailer Loc 29', N'retailer', N'Maple Street 227, Barcelona', 41.360954, 2.164578),
(30, N'Retailer Loc 30', N'retailer', N'Broadway 72, Barcelona', 41.406512, 2.24797),
(31, N'Hub Loc 01', N'hub', N'Sunset Blvd 246, Barcelona', 41.36846, 2.112448),
(32, N'Hub Loc 02', N'hub', N'River Road 60, Barcelona', 41.402681, 2.101809),
(33, N'Hub Loc 03', N'hub', N'King Street 47, Barcelona', 41.37102, 2.100614),
(34, N'Hub Loc 04', N'hub', N'Sunset Blvd 137, Barcelona', 41.37954, 2.184951),
(35, N'Hub Loc 05', N'hub', N'River Road 177, Barcelona', 41.418736, 2.242534),
(36, N'Hub Loc 06', N'hub', N'Central Ave 117, Barcelona', 41.421963, 2.216995),
(37, N'Hub Loc 07', N'hub', N'Broadway 101, Barcelona', 41.381846, 2.159118),
(38, N'Hub Loc 08', N'hub', N'Hillcrest Rd 163, Barcelona', 41.382035, 2.128591),
(39, N'Hub Loc 09', N'hub', N'Oak Lane 113, Barcelona', 41.362984, 2.151008),
(40, N'Hub Loc 10', N'hub', N'Central Ave 27, Barcelona', 41.350019, 2.12269),
(41, N'Hub Loc 11', N'hub', N'Market Street 243, Barcelona', 41.379089, 2.103825),
(42, N'Hub Loc 12', N'hub', N'Oak Lane 158, Barcelona', 41.380098, 2.195161),
(43, N'Hub Loc 13', N'hub', N'Pine Avenue 155, Barcelona', 41.379133, 2.118426),
(44, N'Hub Loc 14', N'hub', N'Hillcrest Rd 120, Barcelona', 41.388432, 2.146778),
(45, N'Hub Loc 15', N'hub', N'River Road 27, Barcelona', 41.409974, 2.211053),
(46, N'Hub Loc 16', N'hub', N'Hillcrest Rd 213, Barcelona', 41.405365, 2.17745),
(47, N'Hub Loc 17', N'hub', N'Oak Lane 244, Barcelona', 41.426162, 2.154263),
(48, N'Hub Loc 18', N'hub', N'Broadway 235, Barcelona', 41.352163, 2.179216),
(49, N'Hub Loc 19', N'hub', N'Market Street 179, Barcelona', 41.417636, 2.17776),
(50, N'Hub Loc 20', N'hub', N'River Road 92, Barcelona', 41.411755, 2.179889),
(51, N'Hub Loc 21', N'hub', N'Broadway 85, Barcelona', 41.400915, 2.191984),
(52, N'Hub Loc 22', N'hub', N'Oak Lane 207, Barcelona', 41.369151, 2.160103),
(53, N'Hub Loc 23', N'hub', N'Oak Lane 52, Barcelona', 41.391411, 2.153334),
(54, N'Hub Loc 24', N'hub', N'Central Ave 8, Barcelona', 41.413209, 2.170836),
(55, N'Hub Loc 25', N'hub', N'Oak Lane 178, Barcelona', 41.398411, 2.151642),
(56, N'Hub Loc 26', N'hub', N'Pine Avenue 245, Barcelona', 41.427961, 2.112081),
(57, N'Hub Loc 27', N'hub', N'Market Street 59, Barcelona', 41.387606, 2.150661),
(58, N'Hub Loc 28', N'hub', N'Hillcrest Rd 160, Barcelona', 41.42882, 2.191539),
(59, N'Hub Loc 29', N'hub', N'Central Ave 123, Barcelona', 41.422736, 2.151601),
(60, N'Hub Loc 30', N'hub', N'Market Street 214, Barcelona', 41.402847, 2.236467),
(61, N'Dropbox Loc 01', N'dropbox', N'Oak Lane 123, Barcelona', 41.421121, 2.165089),
(62, N'Dropbox Loc 02', N'dropbox', N'Pine Avenue 23, Barcelona', 41.414066, 2.245749),
(63, N'Dropbox Loc 03', N'dropbox', N'Sunset Blvd 119, Barcelona', 41.382111, 2.24202),
(64, N'Dropbox Loc 04', N'dropbox', N'River Road 44, Barcelona', 41.429449, 2.104132),
(65, N'Dropbox Loc 05', N'dropbox', N'King Street 232, Barcelona', 41.387228, 2.198379),
(66, N'Dropbox Loc 06', N'dropbox', N'King Street 212, Barcelona', 41.39767, 2.171154),
(67, N'Dropbox Loc 07', N'dropbox', N'Pine Avenue 40, Barcelona', 41.393893, 2.119648),
(68, N'Dropbox Loc 08', N'dropbox', N'Central Ave 205, Barcelona', 41.427671, 2.197451),
(69, N'Dropbox Loc 09', N'dropbox', N'Broadway 192, Barcelona', 41.42469, 2.165071),
(70, N'Dropbox Loc 10', N'dropbox', N'Oak Lane 212, Barcelona', 41.419913, 2.104199),
(71, N'Dropbox Loc 11', N'dropbox', N'Oak Lane 75, Barcelona', 41.390093, 2.214552),
(72, N'Dropbox Loc 12', N'dropbox', N'Pine Avenue 67, Barcelona', 41.393548, 2.225129),
(73, N'Dropbox Loc 13', N'dropbox', N'Central Ave 233, Barcelona', 41.409194, 2.234656),
(74, N'Dropbox Loc 14', N'dropbox', N'King Street 209, Barcelona', 41.422344, 2.163094),
(75, N'Dropbox Loc 15', N'dropbox', N'Broadway 34, Barcelona', 41.392546, 2.178526),
(76, N'Dropbox Loc 16', N'dropbox', N'Central Ave 224, Barcelona', 41.38521, 2.127466),
(77, N'Dropbox Loc 17', N'dropbox', N'Central Ave 199, Barcelona', 41.413934, 2.125852),
(78, N'Dropbox Loc 18', N'dropbox', N'Hillcrest Rd 159, Barcelona', 41.408015, 2.183471),
(79, N'Dropbox Loc 19', N'dropbox', N'Pine Avenue 175, Barcelona', 41.391468, 2.183316),
(80, N'Dropbox Loc 20', N'dropbox', N'Market Street 227, Barcelona', 41.394824, 2.137274),
(81, N'Dropbox Loc 21', N'dropbox', N'Maple Street 11, Barcelona', 41.411781, 2.176157),
(82, N'Dropbox Loc 22', N'dropbox', N'Broadway 8, Barcelona', 41.410799, 2.236873),
(83, N'Dropbox Loc 23', N'dropbox', N'Hillcrest Rd 84, Barcelona', 41.399002, 2.175833),
(84, N'Dropbox Loc 24', N'dropbox', N'Broadway 52, Barcelona', 41.405418, 2.167852),
(85, N'Dropbox Loc 25', N'dropbox', N'Broadway 207, Barcelona', 41.388243, 2.241225),
(86, N'Dropbox Loc 26', N'dropbox', N'Broadway 225, Barcelona', 41.420078, 2.239171),
(87, N'Dropbox Loc 27', N'dropbox', N'Broadway 229, Barcelona', 41.425461, 2.226),
(88, N'Dropbox Loc 28', N'dropbox', N'River Road 107, Barcelona', 41.35973, 2.166318),
(89, N'Dropbox Loc 29', N'dropbox', N'Market Street 172, Barcelona', 41.369251, 2.110968),
(90, N'Dropbox Loc 30', N'dropbox', N'Maple Street 201, Barcelona', 41.359788, 2.21654);
SET IDENTITY_INSERT dbo.locations OFF;

-- INSERTS FOR dbo.retailers (30 rows)
SET IDENTITY_INSERT dbo.retailers ON;
INSERT INTO dbo.retailers (retailer_id, location_id, contact_email) VALUES
(1, 1, N'contact01@retailer.demo'),
(2, 2, N'contact02@retailer.demo'),
(3, 3, N'contact03@retailer.demo'),
(4, 4, N'contact04@retailer.demo'),
(5, 5, N'contact05@retailer.demo'),
(6, 6, N'contact06@retailer.demo'),
(7, 7, N'contact07@retailer.demo'),
(8, 8, N'contact08@retailer.demo'),
(9, 9, N'contact09@retailer.demo'),
(10, 10, N'contact10@retailer.demo'),
(11, 11, N'contact11@retailer.demo'),
(12, 12, N'contact12@retailer.demo'),
(13, 13, N'contact13@retailer.demo'),
(14, 14, N'contact14@retailer.demo'),
(15, 15, N'contact15@retailer.demo'),
(16, 16, N'contact16@retailer.demo'),
(17, 17, N'contact17@retailer.demo'),
(18, 18, N'contact18@retailer.demo'),
(19, 19, N'contact19@retailer.demo'),
(20, 20, N'contact20@retailer.demo'),
(21, 21, N'contact21@retailer.demo'),
(22, 22, N'contact22@retailer.demo'),
(23, 23, N'contact23@retailer.demo'),
(24, 24, N'contact24@retailer.demo'),
(25, 25, N'contact25@retailer.demo'),
(26, 26, N'contact26@retailer.demo'),
(27, 27, N'contact27@retailer.demo'),
(28, 28, N'contact28@retailer.demo'),
(29, 29, N'contact29@retailer.demo'),
(30, 30, N'contact30@retailer.demo');
SET IDENTITY_INSERT dbo.retailers OFF;

-- INSERTS FOR dbo.hubs (30 rows)
SET IDENTITY_INSERT dbo.hubs ON;
INSERT INTO dbo.hubs (hub_id, location_id, washer_model) VALUES
(1, 31, N'Washer-X01'),
(2, 32, N'Washer-X02'),
(3, 33, N'Washer-X03'),
(4, 34, N'Washer-X04'),
(5, 35, N'Washer-X05'),
(6, 36, N'Washer-X06'),
(7, 37, N'Washer-X07'),
(8, 38, N'Washer-X08'),
(9, 39, N'Washer-X09'),
(10, 40, N'Washer-X10'),
(11, 41, N'Washer-X11'),
(12, 42, N'Washer-X12'),
(13, 43, N'Washer-X13'),
(14, 44, N'Washer-X14'),
(15, 45, N'Washer-X15'),
(16, 46, N'Washer-X16'),
(17, 47, N'Washer-X17'),
(18, 48, N'Washer-X18'),
(19, 49, N'Washer-X19'),
(20, 50, N'Washer-X20'),
(21, 51, N'Washer-X21'),
(22, 52, N'Washer-X22'),
(23, 53, N'Washer-X23'),
(24, 54, N'Washer-X24'),
(25, 55, N'Washer-X25'),
(26, 56, N'Washer-X26'),
(27, 57, N'Washer-X27'),
(28, 58, N'Washer-X28'),
(29, 59, N'Washer-X29'),
(30, 60, N'Washer-X30');
SET IDENTITY_INSERT dbo.hubs OFF;

-- INSERTS FOR dbo.customers (30 rows)
SET IDENTITY_INSERT dbo.customers ON;
INSERT INTO dbo.customers (customer_id, name, email, phone, created_at) VALUES
(1, N'Aisha Ahmed', N'aisha.ahmed1@gmail.com', N'+34 6285246444', '2025-09-11 16:28:34'),
(2, N'Bilal Khan', N'bilal.khan2@demo.net', N'+34 6382579162', '2025-08-09 16:28:34'),
(3, N'Carlos Hernandez', N'carlos.hernandez3@demo.net', N'+34 6304753267', '2025-09-08 16:28:34'),
(4, N'Diana Lopez', N'diana.lopez4@demo.net', N'+34 6757774803', '2025-08-16 16:28:34'),
(5, N'Elena Garcia', N'elena.garcia5@demo.net', N'+34 6356983003', '2025-08-19 16:28:34'),
(6, N'Farhan Iqbal', N'farhan.iqbal6@mail.com', N'+34 6561326869', '2025-08-16 16:28:34'),
(7, N'Gabriela Rodriguez', N'gabriela.rodriguez7@demo.net', N'+34 6661303365', '2025-08-10 16:28:34'),
(8, N'Hassan Hussain', N'hassan.hussain8@gmail.com', N'+34 6765956897', '2025-07-25 16:28:34'),
(9, N'Imran Raza', N'imran.raza9@mail.com', N'+34 6244834497', '2025-09-15 16:28:34'),
(10, N'Julia Martinez', N'julia.martinez10@mail.com', N'+34 6435562068', '2025-09-23 16:28:34'),
(11, N'Kamal Saeed', N'kamal.saeed11@outlook.com', N'+34 6443173581', '2025-06-16 16:28:34'),
(12, N'Lina Fernandez', N'lina.fernandez12@demo.net', N'+34 6965338739', '2025-08-08 16:28:34'),
(13, N'Mateo Ruiz', N'mateo.ruiz13@outlook.com', N'+34 6789636619', '2025-07-17 16:28:34'),
(14, N'Nadia Sanchez', N'nadia.sanchez14@demo.net', N'+34 6996486963', '2025-09-17 16:28:34'),
(15, N'Omar Ali', N'omar.ali15@gmail.com', N'+34 6174076002', '2025-08-05 16:28:34'),
(16, N'Paula Ortega', N'paula.ortega16@mail.com', N'+34 6441282389', '2025-07-09 16:28:34'),
(17, N'Qasim Qureshi', N'qasim.qureshi17@mail.com', N'+34 6432404966', '2025-07-13 16:28:34'),
(18, N'Rania Ramos', N'rania.ramos18@outlook.com', N'+34 6185436751', '2025-06-10 16:28:34'),
(19, N'Sami Soto', N'sami.soto19@mail.com', N'+34 6681193715', '2025-08-16 16:28:34'),
(20, N'Tania Tariq', N'tania.tariq20@demo.net', N'+34 6443168032', '2025-09-23 16:28:34'),
(21, N'Usman Uddin', N'usman.uddin21@outlook.com', N'+34 6243708666', '2025-08-26 16:28:34'),
(22, N'Valeria Vega', N'valeria.vega22@mail.com', N'+34 6334385109', '2025-08-20 16:28:34'),
(23, N'Waqas Waris', N'waqas.waris23@gmail.com', N'+34 6774453951', '2025-08-22 16:28:34'),
(24, N'Ximena Xavier', N'ximena.xavier24@demo.net', N'+34 6743984664', '2025-08-25 16:28:34'),
(25, N'Yasir Yunus', N'yasir.yunus25@gmail.com', N'+34 6125201832', '2025-09-24 16:28:34'),
(26, N'Zara Zahid', N'zara.zahid26@mail.com', N'+34 6129483466', '2025-07-20 16:28:34'),
(27, N'Noor Nawaz', N'noor.nawaz27@outlook.com', N'+34 6758965161', '2025-08-28 16:28:34'),
(28, N'Alejandro Alonso', N'alejandro.alonso28@demo.net', N'+34 6238250736', '2025-07-06 16:28:34'),
(29, N'Beatriz Barrios', N'beatriz.barrios29@demo.net', N'+34 6797594889', '2025-07-26 16:28:34'),
(30, N'Hiba Hanan', N'hiba.hanan30@gmail.com', N'+34 6984610140', '2025-08-30 16:28:34');
SET IDENTITY_INSERT dbo.customers OFF;

-- INSERTS FOR dbo.packaging_catalog (30 rows)
SET IDENTITY_INSERT dbo.packaging_catalog ON;
INSERT INTO dbo.packaging_catalog (catalog_id, sku, kind, material, capacity_ml, deposit_amount_cents) VALUES
(1, N'PKG-1001', N'cup', N'pp', 500, 150),
(2, N'PKG-1002', N'box', N'stainless', 330, 250),
(3, N'PKG-1003', N'jar', N'glass', 500, 100),
(4, N'PKG-1004', N'cup', N'pp', 330, 100),
(5, N'PKG-1005', N'box', N'stainless', 250, 200),
(6, N'PKG-1006', N'jar', N'glass', 750, 150),
(7, N'PKG-1007', N'cup', N'pp', 250, 100),
(8, N'PKG-1008', N'box', N'stainless', 750, 300),
(9, N'PKG-1009', N'jar', N'glass', 500, 300),
(10, N'PKG-1010', N'cup', N'pp', 330, 200),
(11, N'PKG-1011', N'box', N'stainless', 250, 250),
(12, N'PKG-1012', N'jar', N'glass', 330, 150),
(13, N'PKG-1013', N'cup', N'pp', 500, 250),
(14, N'PKG-1014', N'box', N'stainless', 250, 200),
(15, N'PKG-1015', N'jar', N'glass', 500, 200),
(16, N'PKG-1016', N'cup', N'pp', 1000, 200),
(17, N'PKG-1017', N'box', N'stainless', 330, 100),
(18, N'PKG-1018', N'jar', N'glass', 500, 150),
(19, N'PKG-1019', N'cup', N'pp', 500, 150),
(20, N'PKG-1020', N'box', N'stainless', 250, 200),
(21, N'PKG-1021', N'jar', N'glass', 750, 100),
(22, N'PKG-1022', N'cup', N'pp', 750, 200),
(23, N'PKG-1023', N'box', N'stainless', 1000, 150),
(24, N'PKG-1024', N'jar', N'glass', 330, 300),
(25, N'PKG-1025', N'cup', N'pp', 250, 100),
(26, N'PKG-1026', N'box', N'stainless', 500, 100),
(27, N'PKG-1027', N'jar', N'glass', 330, 250),
(28, N'PKG-1028', N'cup', N'pp', 1000, 100),
(29, N'PKG-1029', N'box', N'stainless', 750, 100),
(30, N'PKG-1030', N'jar', N'glass', 500, 200);
SET IDENTITY_INSERT dbo.packaging_catalog OFF;

-- INSERTS FOR dbo.packaging_instances (30 rows)
SET IDENTITY_INSERT dbo.packaging_instances ON;
INSERT INTO dbo.packaging_instances (instance_id, catalog_id, uid_code, state, birthed_at, retired_at) VALUES
(1, 1, N'UID-0001-4814', N'in_use', '2025-01-12 16:28:34', NULL),
(2, 2, N'UID-0002-9670', N'at_retailer', '2024-12-24 16:28:34', NULL),
(3, 3, N'UID-0003-7381', N'damaged', '2024-12-08 16:28:34', NULL),
(4, 4, N'UID-0004-9096', N'at_retailer', '2025-03-30 16:28:34', NULL),
(5, 5, N'UID-0005-3371', N'available', '2024-11-11 16:28:34', NULL),
(6, 6, N'UID-0006-9404', N'lost', '2024-12-05 16:28:34', NULL),
(7, 7, N'UID-0007-9282', N'at_retailer', '2024-10-21 16:28:34', NULL),
(8, 8, N'UID-0008-9581', N'available', '2024-11-11 16:28:34', NULL),
(9, 9, N'UID-0009-4767', N'in_use', '2025-06-03 16:28:34', NULL),
(10, 10, N'UID-0010-1685', N'at_retailer', '2024-12-29 16:28:34', NULL),
(11, 11, N'UID-0011-6909', N'in_use', '2025-03-06 16:28:34', NULL),
(12, 12, N'UID-0012-8395', N'available', '2025-01-01 16:28:34', NULL),
(13, 13, N'UID-0013-1308', N'at_hub', '2025-02-05 16:28:34', NULL),
(14, 14, N'UID-0014-5321', N'available', '2025-02-14 16:28:34', NULL),
(15, 15, N'UID-0015-2148', N'in_use', '2024-12-24 16:28:34', NULL),
(16, 16, N'UID-0016-9617', N'in_use', '2024-12-02 16:28:34', NULL),
(17, 17, N'UID-0017-8763', N'washing', '2024-11-15 16:28:34', NULL),
(18, 18, N'UID-0018-2219', N'washing', '2025-04-11 16:28:34', NULL),
(19, 19, N'UID-0019-4362', N'at_hub', '2024-12-03 16:28:34', NULL),
(20, 20, N'UID-0020-8542', N'retired', '2024-11-06 16:28:34', '2025-09-25 16:28:34'),
(21, 21, N'UID-0021-2257', N'retired', '2024-10-20 16:28:34', '2025-09-16 16:28:34'),
(22, 22, N'UID-0022-5707', N'available', '2025-01-04 16:28:34', NULL),
(23, 23, N'UID-0023-4248', N'in_use', '2025-01-08 16:28:34', NULL),
(24, 24, N'UID-0024-3415', N'damaged', '2025-04-06 16:28:34', NULL),
(25, 25, N'UID-0025-5987', N'at_retailer', '2025-06-07 16:28:34', NULL),
(26, 26, N'UID-0026-8903', N'available', '2025-02-06 16:28:34', NULL),
(27, 27, N'UID-0027-5403', N'in_use', '2024-12-15 16:28:34', NULL),
(28, 28, N'UID-0028-4566', N'retired', '2025-03-28 16:28:34', '2025-09-15 16:28:34'),
(29, 29, N'UID-0029-9462', N'washing', '2025-02-12 16:28:34', NULL),
(30, 30, N'UID-0030-8633', N'retired', '2024-11-26 16:28:34', '2025-10-04 16:28:34');
SET IDENTITY_INSERT dbo.packaging_instances OFF;

-- INSERTS FOR dbo.deposit_accounts (30 rows)
SET IDENTITY_INSERT dbo.deposit_accounts ON;
INSERT INTO dbo.deposit_accounts (account_id, customer_id, balance_cents) VALUES
(1, 1, 1000),
(2, 2, 0),
(3, 3, 0),
(4, 4, 0),
(5, 5, 500),
(6, 6, 0),
(7, 7, 0),
(8, 8, 500),
(9, 9, 0),
(10, 10, 2000),
(11, 11, 1000),
(12, 12, 500),
(13, 13, 0),
(14, 14, 500),
(15, 15, 0),
(16, 16, 0),
(17, 17, 0),
(18, 18, 1000),
(19, 19, 0),
(20, 20, 0),
(21, 21, 1500),
(22, 22, 1000),
(23, 23, 0),
(24, 24, 0),
(25, 25, 0),
(26, 26, 1000),
(27, 27, 2000),
(28, 28, 1500),
(29, 29, 1000),
(30, 30, 0);
SET IDENTITY_INSERT dbo.deposit_accounts OFF;

-- INSERTS FOR dbo.deposit_transactions (30 rows)
SET IDENTITY_INSERT dbo.deposit_transactions ON;
INSERT INTO dbo.deposit_transactions (tx_id, account_id, delta_cents, reason, ref_table, ref_id, created_at) VALUES
(1, 1, 200, N'penalty', NULL, NULL, '2025-09-09 16:28:34'),
(2, 2, -100, N'adjustment', NULL, NULL, '2025-08-19 16:28:34'),
(3, 3, 200, N'return_release', NULL, NULL, '2025-10-08 16:28:34'),
(4, 4, -100, N'adjustment', NULL, NULL, '2025-08-18 16:28:34'),
(5, 5, 100, N'return_release', NULL, NULL, '2025-08-16 16:28:34'),
(6, 6, 100, N'adjustment', NULL, NULL, '2025-08-29 16:28:34'),
(7, 7, 200, N'penalty', NULL, NULL, '2025-10-08 16:28:34'),
(8, 8, 100, N'penalty', NULL, NULL, '2025-08-19 16:28:34'),
(9, 9, 200, N'return_release', NULL, NULL, '2025-10-07 16:28:34'),
(10, 10, -200, N'penalty', NULL, NULL, '2025-09-06 16:28:34'),
(11, 11, 100, N'checkout_hold', NULL, NULL, '2025-08-19 16:28:34'),
(12, 12, -100, N'checkout_hold', NULL, NULL, '2025-08-23 16:28:34'),
(13, 13, -100, N'penalty', NULL, NULL, '2025-10-02 16:28:34'),
(14, 14, 100, N'checkout_hold', NULL, NULL, '2025-10-02 16:28:34'),
(15, 15, 250, N'penalty', NULL, NULL, '2025-07-19 16:28:34'),
(16, 16, 150, N'return_release', NULL, NULL, '2025-09-04 16:28:34'),
(17, 17, -100, N'penalty', NULL, NULL, '2025-09-14 16:28:34'),
(18, 18, 250, N'penalty', NULL, NULL, '2025-08-15 16:28:34'),
(19, 19, 200, N'adjustment', NULL, NULL, '2025-07-30 16:28:34'),
(20, 20, -150, N'return_release', NULL, NULL, '2025-09-28 16:28:34'),
(21, 21, 200, N'adjustment', NULL, NULL, '2025-08-12 16:28:34'),
(22, 22, -150, N'return_release', NULL, NULL, '2025-07-18 16:28:34'),
(23, 23, 250, N'penalty', NULL, NULL, '2025-08-07 16:28:34'),
(24, 24, 200, N'return_release', NULL, NULL, '2025-09-17 16:28:34'),
(25, 25, -100, N'adjustment', NULL, NULL, '2025-08-26 16:28:34'),
(26, 26, 100, N'penalty', NULL, NULL, '2025-09-06 16:28:34'),
(27, 27, -200, N'penalty', NULL, NULL, '2025-08-18 16:28:34'),
(28, 28, -200, N'return_release', NULL, NULL, '2025-08-31 16:28:34'),
(29, 29, -100, N'adjustment', NULL, NULL, '2025-09-23 16:28:34'),
(30, 30, 150, N'return_release', NULL, NULL, '2025-09-29 16:28:34');
SET IDENTITY_INSERT dbo.deposit_transactions OFF;

-- INSERTS FOR dbo.checkouts (30 rows)
SET IDENTITY_INSERT dbo.checkouts ON;
INSERT INTO dbo.checkouts (checkout_id, instance_id, retailer_id, customer_id, checkout_time, due_back_days) VALUES
(1, 1, 1, 6, '2025-09-20 16:28:34', 14),
(2, 2, 2, 7, '2025-09-02 16:28:34', 14),
(3, 3, 3, 8, '2025-09-19 16:28:34', 10),
(4, 4, 4, 9, '2025-09-12 16:28:34', 10),
(5, 5, 5, 10, '2025-09-06 16:28:34', 7),
(6, 6, 6, 11, '2025-08-29 16:28:34', 7),
(7, 7, 7, 12, '2025-09-18 16:28:34', 7),
(8, 8, 8, 13, '2025-09-22 16:28:34', 10),
(9, 9, 9, 14, '2025-08-29 16:28:34', 7),
(10, 10, 10, 15, '2025-09-13 16:28:34', 7),
(11, 11, 11, 16, '2025-09-10 16:28:34', 10),
(12, 12, 12, 17, '2025-09-21 16:28:34', 7),
(13, 13, 13, 18, '2025-09-07 16:28:34', 10),
(14, 14, 14, 19, '2025-09-07 16:28:34', 14),
(15, 15, 15, 20, '2025-08-31 16:28:34', 7),
(16, 16, 16, 21, '2025-09-09 16:28:34', 10),
(17, 17, 17, 22, '2025-09-12 16:28:34', 7),
(18, 18, 18, 23, '2025-09-02 16:28:34', 10),
(19, 19, 19, 24, '2025-09-10 16:28:34', 7),
(20, 20, 20, 25, '2025-09-01 16:28:34', 14),
(21, 21, 21, 26, '2025-09-20 16:28:34', 7),
(22, 22, 22, 27, '2025-09-16 16:28:34', 7),
(23, 23, 23, 28, '2025-09-09 16:28:34', 10),
(24, 24, 24, 29, '2025-09-05 16:28:34', 10),
(25, 25, 25, 30, '2025-09-14 16:28:34', 7),
(26, 26, 26, 1, '2025-09-25 16:28:34', 7),
(27, 27, 27, 2, '2025-09-06 16:28:34', 14),
(28, 28, 28, 3, '2025-09-03 16:28:34', 14),
(29, 29, 29, 4, '2025-09-02 16:28:34', 7),
(30, 30, 30, 5, '2025-09-29 16:28:34', 10);
SET IDENTITY_INSERT dbo.checkouts OFF;

-- INSERTS FOR dbo.returns (30 rows)
SET IDENTITY_INSERT dbo.returns ON;
INSERT INTO dbo.returns (return_id, instance_id, customer_id, location_id, return_time, checkout_id) VALUES
(1, 1, 6, 30, '2025-09-22 16:28:34', 1),
(2, 2, 7, 28, '2025-09-24 16:28:34', 2),
(3, 3, 8, 15, '2025-10-01 16:28:34', 3),
(4, 4, 9, 26, '2025-10-05 16:28:34', 4),
(5, 5, 10, 8, '2025-10-04 16:28:34', 5),
(6, 6, 11, 5, '2025-09-22 16:28:34', 6),
(7, 7, 12, 22, '2025-10-05 16:28:34', 7),
(8, 8, 13, 27, '2025-09-18 16:28:34', 8),
(9, 9, 14, 28, '2025-09-24 16:28:34', 9),
(10, 10, 15, 3, '2025-09-21 16:28:34', 10),
(11, 11, 16, 25, '2025-10-07 16:28:34', 11),
(12, 12, 17, 1, '2025-10-04 16:28:34', 12),
(13, 13, 18, 8, '2025-09-20 16:28:34', 13),
(14, 14, 19, 30, '2025-10-07 16:28:34', 14),
(15, 15, 20, 21, '2025-09-29 16:28:34', 15),
(16, 16, 21, 5, '2025-09-18 16:28:34', 16),
(17, 17, 22, 9, '2025-09-22 16:28:34', 17),
(18, 18, 23, 21, '2025-09-25 16:28:34', 18),
(19, 19, 24, 23, '2025-10-05 16:28:34', 19),
(20, 20, 25, 4, '2025-10-06 16:28:34', 20),
(21, 21, NULL, 70, '2025-09-22 16:28:34', NULL),
(22, 22, NULL, 79, '2025-10-02 16:28:34', NULL),
(23, 23, NULL, 73, '2025-09-30 16:28:34', NULL),
(24, 24, NULL, 68, '2025-09-19 16:28:34', NULL),
(25, 25, NULL, 61, '2025-10-08 16:28:34', NULL),
(26, 26, NULL, 78, '2025-09-29 16:28:34', NULL),
(27, 27, NULL, 75, '2025-09-30 16:28:34', NULL),
(28, 28, NULL, 71, '2025-09-18 16:28:34', NULL),
(29, 29, NULL, 87, '2025-10-01 16:28:34', NULL),
(30, 30, NULL, 76, '2025-09-22 16:28:34', NULL);
SET IDENTITY_INSERT dbo.returns OFF;

-- INSERTS FOR dbo.wash_cycles (30 rows)
SET IDENTITY_INSERT dbo.wash_cycles ON;
INSERT INTO dbo.wash_cycles (wash_id, hub_id, batch_code, start_time, end_time, temp_c, detergent) VALUES
(1, 1, N'W001', '2025-10-04 16:28:34', '2025-10-08 16:28:34', 74.21, N'EcoClean'),
(2, 2, N'W002', '2025-10-07 16:28:34', NULL, 64.97, N'ProWash'),
(3, 3, N'W003', '2025-10-06 16:28:34', '2025-10-08 16:28:34', 73.5, N'D2'),
(4, 4, N'W004', '2025-09-30 16:28:34', NULL, 61.76, N'ProWash'),
(5, 5, N'W005', '2025-10-02 16:28:34', '2025-10-08 16:28:34', 55.14, N'EcoClean'),
(6, 6, N'W006', '2025-09-26 16:28:34', '2025-10-08 16:28:34', 59.1, N'D2'),
(7, 7, N'W007', '2025-10-03 16:28:34', '2025-10-08 16:28:34', 59.62, N'D2'),
(8, 8, N'W008', '2025-10-03 16:28:34', '2025-10-08 16:28:34', 57.18, N'ProWash'),
(9, 9, N'W009', '2025-09-28 16:28:34', NULL, 59.47, N'ProWash'),
(10, 10, N'W010', '2025-09-23 16:28:34', '2025-10-08 16:28:34', 73.44, N'D1'),
(11, 11, N'W011', '2025-10-04 16:28:34', NULL, 66.92, N'ProWash'),
(12, 12, N'W012', '2025-10-07 16:28:34', '2025-10-08 16:28:34', 62.87, N'EcoClean'),
(13, 13, N'W013', '2025-09-26 16:28:34', NULL, 56.59, N'D2'),
(14, 14, N'W014', '2025-10-02 16:28:34', NULL, 68.05, N'ProWash'),
(15, 15, N'W015', '2025-10-07 16:28:34', '2025-10-08 16:28:34', 71.78, N'EcoClean'),
(16, 16, N'W016', '2025-09-30 16:28:34', NULL, 55.06, N'EcoClean'),
(17, 17, N'W017', '2025-10-06 16:28:34', '2025-10-08 16:28:34', 66.22, N'D2'),
(18, 18, N'W018', '2025-10-01 16:28:34', '2025-10-08 16:28:34', 71.44, N'ProWash'),
(19, 19, N'W019', '2025-10-06 16:28:34', NULL, 64.47, N'EcoClean'),
(20, 20, N'W020', '2025-09-29 16:28:34', '2025-10-08 16:28:34', 61.47, N'ProWash'),
(21, 21, N'W021', '2025-10-07 16:28:34', '2025-10-08 16:28:34', 71.24, N'ProWash'),
(22, 22, N'W022', '2025-10-07 16:28:34', '2025-10-08 16:28:34', 56.25, N'D1'),
(23, 23, N'W023', '2025-10-03 16:28:34', NULL, 56.26, N'EcoClean'),
(24, 24, N'W024', '2025-10-02 16:28:34', '2025-10-08 16:28:34', 60.24, N'EcoClean'),
(25, 25, N'W025', '2025-09-23 16:28:34', '2025-10-08 16:28:34', 69.43, N'D1'),
(26, 26, N'W026', '2025-10-07 16:28:34', '2025-10-08 16:28:34', 64.5, N'ProWash'),
(27, 27, N'W027', '2025-09-25 16:28:34', '2025-10-08 16:28:34', 73.27, N'ProWash'),
(28, 28, N'W028', '2025-10-05 16:28:34', '2025-10-08 16:28:34', 55.17, N'EcoClean'),
(29, 29, N'W029', '2025-09-24 16:28:34', '2025-10-08 16:28:34', 67.15, N'EcoClean'),
(30, 30, N'W030', '2025-09-24 16:28:34', '2025-10-08 16:28:34', 70.68, N'D1');
SET IDENTITY_INSERT dbo.wash_cycles OFF;

-- INSERTS FOR dbo.inspections (30 rows)
SET IDENTITY_INSERT dbo.inspections ON;
INSERT INTO dbo.inspections (inspection_id, instance_id, wash_id, inspector, result, notes, inspected_at) VALUES
(1, 1, 1, N'Esha', N'pass', N'Label faded', '2025-10-06 16:28:34'),
(2, 2, 2, N'Bob', N'fail', N'OK', '2025-09-28 16:28:34'),
(3, 3, 3, N'Alice', N'fail', N'Rewash advised', '2025-10-06 16:28:34'),
(4, 4, 4, N'David', N'pass', N'OK', '2025-10-04 16:28:34'),
(5, 5, 5, N'Esha', N'pass', N'Minor scratches', '2025-10-07 16:28:34'),
(6, 6, 6, N'David', N'fail', N'Label faded', '2025-10-06 16:28:34'),
(7, 7, 7, N'Bob', N'pass', N'Label faded', '2025-10-01 16:28:34'),
(8, 8, 8, N'Esha', N'pass', N'OK', '2025-10-04 16:28:34'),
(9, 9, 9, N'Carla', N'pass', N'Rewash advised', '2025-10-03 16:28:34'),
(10, 10, 10, N'Carla', N'pass', N'Minor scratches', '2025-10-01 16:28:34'),
(11, 11, 11, N'Bob', N'pass', N'Minor scratches', '2025-10-05 16:28:34'),
(12, 12, 12, N'Bob', N'pass', N'Minor scratches', '2025-10-03 16:28:34'),
(13, 13, 13, N'Alice', N'fail', N'Rewash advised', '2025-10-05 16:28:34'),
(14, 14, 14, N'Esha', N'pass', N'OK', '2025-09-28 16:28:34'),
(15, 15, 15, N'David', N'pass', N'OK', '2025-10-08 16:28:34'),
(16, 16, 16, N'David', N'pass', N'Label faded', '2025-10-03 16:28:34'),
(17, 17, 17, N'Alice', N'pass', N'Minor scratches', '2025-10-07 16:28:34'),
(18, 18, 18, N'Alice', N'pass', N'Minor scratches', '2025-10-07 16:28:34'),
(19, 19, 19, N'Carla', N'pass', N'Label faded', '2025-09-29 16:28:34'),
(20, 20, 20, N'Carla', N'pass', N'OK', '2025-09-28 16:28:34'),
(21, 21, 21, N'Esha', N'pass', N'Minor scratches', '2025-10-08 16:28:34'),
(22, 22, 22, N'Carla', N'pass', N'Minor scratches', '2025-10-08 16:28:34'),
(23, 23, 23, N'Bob', N'pass', N'OK', '2025-09-29 16:28:34'),
(24, 24, 24, N'Fahad', N'pass', N'OK', '2025-10-03 16:28:34'),
(25, 25, 25, N'David', N'pass', N'Minor scratches', '2025-09-29 16:28:34'),
(26, 26, 26, N'Carla', N'pass', N'Minor scratches', '2025-10-08 16:28:34'),
(27, 27, 27, N'David', N'fail', N'OK', '2025-10-02 16:28:34'),
(28, 28, 28, N'Alice', N'fail', N'Minor scratches', '2025-09-28 16:28:34'),
(29, 29, 29, N'Esha', N'pass', N'Minor scratches', '2025-10-02 16:28:34'),
(30, 30, 30, N'Fahad', N'pass', N'Label faded', '2025-10-04 16:28:34');
SET IDENTITY_INSERT dbo.inspections OFF;

-- INSERTS FOR dbo.contamination_incidents (30 rows)
SET IDENTITY_INSERT dbo.contamination_incidents ON;
INSERT INTO dbo.contamination_incidents (incident_id, instance_id, kind, severity, description, detected_at) VALUES
(1, 1, N'foreign_matter', 3, N'pH anomaly', '2025-09-08 16:28:34'),
(2, 2, N'microbial', 3, N'Visual stain', '2025-09-10 16:28:34'),
(3, 3, N'chemical', 4, N'pH anomaly', '2025-10-08 16:28:34'),
(4, 4, N'chemical', 2, N'pH anomaly', '2025-09-15 16:28:34'),
(5, 5, N'chemical', 2, N'Residue detected', '2025-09-25 16:28:34'),
(6, 6, N'microbial', 4, N'Residue detected', '2025-09-12 16:28:34'),
(7, 7, N'microbial', 4, N'Visual stain', '2025-09-10 16:28:34'),
(8, 8, N'chemical', 4, N'Smell detected', '2025-10-04 16:28:34'),
(9, 9, N'microbial', 1, N'Visual stain', '2025-10-04 16:28:34'),
(10, 10, N'foreign_matter', 4, N'Residue detected', '2025-09-20 16:28:34'),
(11, 11, N'foreign_matter', 3, N'Visual stain', '2025-10-03 16:28:34'),
(12, 12, N'microbial', 3, N'Particle found', '2025-10-03 16:28:34'),
(13, 13, N'foreign_matter', 2, N'Residue detected', '2025-10-05 16:28:34'),
(14, 14, N'chemical', 4, N'Smell detected', '2025-09-29 16:28:34'),
(15, 15, N'microbial', 1, N'pH anomaly', '2025-09-28 16:28:34'),
(16, 16, N'microbial', 5, N'pH anomaly', '2025-10-06 16:28:34'),
(17, 17, N'foreign_matter', 5, N'Smell detected', '2025-09-18 16:28:34'),
(18, 18, N'microbial', 5, N'pH anomaly', '2025-09-19 16:28:34'),
(19, 19, N'microbial', 4, N'Smell detected', '2025-09-20 16:28:34'),
(20, 20, N'microbial', 1, N'pH anomaly', '2025-09-08 16:28:34'),
(21, 21, N'foreign_matter', 2, N'pH anomaly', '2025-09-27 16:28:34'),
(22, 22, N'microbial', 2, N'Smell detected', '2025-09-15 16:28:34'),
(23, 23, N'microbial', 1, N'Visual stain', '2025-09-12 16:28:34'),
(24, 24, N'foreign_matter', 1, N'Particle found', '2025-10-05 16:28:34'),
(25, 25, N'chemical', 5, N'pH anomaly', '2025-09-21 16:28:34'),
(26, 26, N'foreign_matter', 3, N'pH anomaly', '2025-09-29 16:28:34'),
(27, 27, N'foreign_matter', 2, N'pH anomaly', '2025-09-26 16:28:34'),
(28, 28, N'foreign_matter', 3, N'pH anomaly', '2025-09-22 16:28:34'),
(29, 29, N'chemical', 2, N'Residue detected', '2025-10-08 16:28:34'),
(30, 30, N'foreign_matter', 4, N'pH anomaly', '2025-10-01 16:28:34');
SET IDENTITY_INSERT dbo.contamination_incidents OFF;

-- INSERTS FOR dbo.sensor_readings (30 rows)
SET IDENTITY_INSERT dbo.sensor_readings ON;
INSERT INTO dbo.sensor_readings (reading_id, instance_id, location_id, sensor_type, value, measured_at) VALUES
(1, 1, 55, N'humidity', 74.6, '2025-10-01 16:28:34'),
(2, 2, 26, N'shock', 2.0, '2025-10-07 16:28:34'),
(3, 3, 12, N'shock', 1.83, '2025-10-01 16:28:34'),
(4, 4, 77, N'humidity', 22.85, '2025-10-06 16:28:34'),
(5, 5, 30, N'humidity', 41.96, '2025-10-07 16:28:34'),
(6, 6, 25, N'humidity', 82.64, '2025-10-06 16:28:34'),
(7, 7, 28, N'temperature', 39.85, '2025-10-07 16:28:34'),
(8, 8, 5, N'shock', 1.44, '2025-10-06 16:28:34'),
(9, 9, 86, N'humidity', 85.14, '2025-10-07 16:28:34'),
(10, 10, 50, N'shock', 0.79, '2025-10-04 16:28:34'),
(11, 11, 35, N'shock', 2.51, '2025-10-01 16:28:34'),
(12, 12, 19, N'shock', 3.08, '2025-10-05 16:28:34'),
(13, 13, 42, N'temperature', 9.56, '2025-10-02 16:28:34'),
(14, 14, 21, N'shock', 3.4, '2025-10-02 16:28:34'),
(15, 15, 26, N'shock', 0.58, '2025-10-08 16:28:34'),
(16, 16, 88, N'shock', 4.83, '2025-10-01 16:28:34'),
(17, 17, 77, N'humidity', 68.21, '2025-10-07 16:28:34'),
(18, 18, 48, N'humidity', 79.96, '2025-10-03 16:28:34'),
(19, 19, 43, N'shock', 2.89, '2025-10-03 16:28:34'),
(20, 20, 55, N'temperature', 18.81, '2025-10-06 16:28:34'),
(21, 21, 84, N'temperature', 13.26, '2025-10-04 16:28:34'),
(22, 22, 51, N'humidity', 84.99, '2025-10-03 16:28:34'),
(23, 23, 61, N'humidity', 22.37, '2025-10-06 16:28:34'),
(24, 24, 50, N'humidity', 50.26, '2025-10-03 16:28:34'),
(25, 25, 5, N'shock', 1.14, '2025-10-08 16:28:34'),
(26, 26, 2, N'temperature', 23.55, '2025-10-04 16:28:34'),
(27, 27, 17, N'shock', 2.67, '2025-10-02 16:28:34'),
(28, 28, 70, N'humidity', 29.36, '2025-10-03 16:28:34'),
(29, 29, 87, N'shock', 0.79, '2025-10-08 16:28:34'),
(30, 30, 23, N'temperature', 19.13, '2025-10-07 16:28:34');
SET IDENTITY_INSERT dbo.sensor_readings OFF;

-- INSERTS FOR dbo.movements (30 rows)
SET IDENTITY_INSERT dbo.movements ON;
INSERT INTO dbo.movements (mv_id, instance_id, from_loc_id, to_loc_id, moved_at, note) VALUES
(1, 1, 82, 19, '2025-09-30 16:28:34', N'customer pickup'),
(2, 2, 34, 2, '2025-10-07 16:28:34', N'restock'),
(3, 3, 45, 77, '2025-09-18 16:28:34', N'restock'),
(4, 4, 57, 78, '2025-09-22 16:28:34', N'customer pickup'),
(5, 5, 32, 22, '2025-10-08 16:28:34', N'to retailer'),
(6, 6, 8, 69, '2025-10-08 16:28:34', N'customer pickup'),
(7, 7, 24, 31, '2025-10-03 16:28:34', N'to retailer'),
(8, 8, 14, 2, '2025-09-19 16:28:34', N'restock'),
(9, 9, 85, 26, '2025-10-04 16:28:34', N'customer pickup'),
(10, 10, 26, 67, '2025-09-19 16:28:34', N'restock'),
(11, 11, 83, 83, '2025-09-25 16:28:34', N'restock'),
(12, 12, 23, 66, '2025-09-29 16:28:34', N'to retailer'),
(13, 13, 39, 81, '2025-10-07 16:28:34', N'customer pickup'),
(14, 14, 69, 1, '2025-09-26 16:28:34', N'customer pickup'),
(15, 15, 60, 11, '2025-09-18 16:28:34', N'customer pickup'),
(16, 16, 23, 29, '2025-10-05 16:28:34', N'to dropbox'),
(17, 17, 30, 83, '2025-10-07 16:28:34', N'to retailer'),
(18, 18, 43, 89, '2025-09-30 16:28:34', N'to retailer'),
(19, 19, 35, 82, '2025-09-21 16:28:34', N'customer pickup'),
(20, 20, 88, 67, '2025-09-30 16:28:34', N'to dropbox'),
(21, 21, 83, 28, '2025-10-06 16:28:34', N'restock'),
(22, 22, 2, 22, '2025-09-30 16:28:34', N'to hub'),
(23, 23, 26, 21, '2025-09-28 16:28:34', N'to hub'),
(24, 24, 50, 43, '2025-09-19 16:28:34', N'to hub'),
(25, 25, 49, 81, '2025-09-21 16:28:34', N'customer pickup'),
(26, 26, 61, 68, '2025-10-08 16:28:34', N'to retailer'),
(27, 27, 56, 30, '2025-09-20 16:28:34', N'to dropbox'),
(28, 28, 28, 51, '2025-09-19 16:28:34', N'restock'),
(29, 29, 10, 73, '2025-10-03 16:28:34', N'to hub'),
(30, 30, 5, 4, '2025-10-05 16:28:34', N'to retailer');
SET IDENTITY_INSERT dbo.movements OFF;

-- INSERTS FOR dbo.audit_logs (30 rows)
SET IDENTITY_INSERT dbo.audit_logs ON;
INSERT INTO dbo.audit_logs (log_id, entity_type, entity_id, event_type, detail, created_at) VALUES
(1, N'return', 12, N'PENALTY', N'{"note":"auto log 1","by":"system"}', '2025-09-16 16:28:34'),
(2, N'instance', 1, N'STATE_CHANGE', N'{"note":"auto log 2","by":"system"}', '2025-10-04 16:28:34'),
(3, N'instance', 23, N'STATE_CHANGE', N'{"note":"auto log 3","by":"system"}', '2025-09-15 16:28:34'),
(4, N'instance', 3, N'ADJUST', N'{"note":"auto log 4","by":"system"}', '2025-10-02 16:28:34'),
(5, N'contamination', 29, N'STATE_CHANGE', N'{"note":"auto log 5","by":"system"}', '2025-09-10 16:28:34'),
(6, N'deposit', 4, N'PENALTY', N'{"note":"auto log 6","by":"system"}', '2025-10-02 16:28:34'),
(7, N'wash', 4, N'STATE_CHANGE', N'{"note":"auto log 7","by":"system"}', '2025-10-07 16:28:34'),
(8, N'checkout', 27, N'ADJUST', N'{"note":"auto log 8","by":"system"}', '2025-09-23 16:28:34'),
(9, N'checkout', 5, N'STATE_CHANGE', N'{"note":"auto log 9","by":"system"}', '2025-09-13 16:28:34'),
(10, N'wash', 10, N'ADJUST', N'{"note":"auto log 10","by":"system"}', '2025-09-28 16:28:34'),
(11, N'deposit', 9, N'STATE_CHANGE', N'{"note":"auto log 11","by":"system"}', '2025-09-27 16:28:34'),
(12, N'inspection', 30, N'ADJUST', N'{"note":"auto log 12","by":"system"}', '2025-10-07 16:28:34'),
(13, N'movement', 30, N'ADJUST', N'{"note":"auto log 13","by":"system"}', '2025-09-14 16:28:34'),
(14, N'contamination', 16, N'ADJUST', N'{"note":"auto log 14","by":"system"}', '2025-09-19 16:28:34'),
(15, N'instance', 26, N'NOTE', N'{"note":"auto log 15","by":"system"}', '2025-10-08 16:28:34'),
(16, N'deposit', 17, N'STATE_CHANGE', N'{"note":"auto log 16","by":"system"}', '2025-09-27 16:28:34'),
(17, N'sensor', 23, N'STATE_CHANGE', N'{"note":"auto log 17","by":"system"}', '2025-09-21 16:28:34'),
(18, N'wash', 23, N'STATE_CHANGE', N'{"note":"auto log 18","by":"system"}', '2025-09-20 16:28:34'),
(19, N'inspection', 6, N'NOTE', N'{"note":"auto log 19","by":"system"}', '2025-10-08 16:28:34'),
(20, N'contamination', 7, N'ADJUST', N'{"note":"auto log 20","by":"system"}', '2025-09-14 16:28:34'),
(21, N'instance', 1, N'ADJUST', N'{"note":"auto log 21","by":"system"}', '2025-09-23 16:28:34'),
(22, N'checkout', 16, N'PENALTY', N'{"note":"auto log 22","by":"system"}', '2025-09-08 16:28:34'),
(23, N'sensor', 19, N'ADJUST', N'{"note":"auto log 23","by":"system"}', '2025-09-08 16:28:34'),
(24, N'contamination', 9, N'PENALTY', N'{"note":"auto log 24","by":"system"}', '2025-09-29 16:28:34'),
(25, N'wash', 23, N'PENALTY', N'{"note":"auto log 25","by":"system"}', '2025-09-23 16:28:34'),
(26, N'return', 4, N'STATE_CHANGE', N'{"note":"auto log 26","by":"system"}', '2025-09-23 16:28:34'),
(27, N'contamination', 26, N'STATE_CHANGE', N'{"note":"auto log 27","by":"system"}', '2025-09-18 16:28:34'),
(28, N'movement', 12, N'STATE_CHANGE', N'{"note":"auto log 28","by":"system"}', '2025-09-26 16:28:34'),
(29, N'deposit', 29, N'STATE_CHANGE', N'{"note":"auto log 29","by":"system"}', '2025-09-25 16:28:34'),
(30, N'instance', 12, N'PENALTY', N'{"note":"auto log 30","by":"system"}', '2025-09-29 16:28:34');
SET IDENTITY_INSERT dbo.audit_logs OFF;



- 1) Count by location kind
SELECT kind, COUNT(*) AS total
FROM dbo.locations
GROUP BY kind
ORDER BY total DESC;

-- 2) Retailers with their coordinates
SELECT r.retailer_id, l.name AS retailer_name, l.address, l.lat, l.lng
FROM dbo.retailers r
JOIN dbo.locations l ON l.location_id = r.location_id
ORDER BY r.retailer_id;

-- 3) Hubs list
SELECT h.hub_id, l.name AS hub_name, h.washer_model, l.address
FROM dbo.hubs h
JOIN dbo.locations l ON l.location_id = h.location_id
ORDER BY h.hub_id;

-- 4) Nearby search (within rough bounding box) around a point
DECLARE @lat DECIMAL(9,6) = 41.385000, @lng DECIMAL(9,6) = 2.170000;
SELECT location_id, name, kind, lat, lng
FROM dbo.locations
WHERE lat BETWEEN @lat - 0.05 AND @lat + 0.05
  AND lng BETWEEN @lng - 0.06 AND @lng + 0.06
ORDER BY name;

-- 5) Precise nearest retailers by Haversine (top 10 within 5 km)
DECLARE @EarthKm FLOAT = 6371.0, @maxKm FLOAT = 5.0;
SELECT TOP 10
  l.location_id, l.name, l.address,
  @EarthKm * ACOS(
    COS(RADIANS(@lat)) * COS(RADIANS(l.lat)) *
    COS(RADIANS(l.lng - @lng)) +
    SIN(RADIANS(@lat)) * SIN(RADIANS(l.lat))
  ) AS distance_km
FROM dbo.locations l
JOIN dbo.retailers r ON r.location_id = l.location_id
WHERE l.lat IS NOT NULL AND l.lng IS NOT NULL
ORDER BY distance_km ASC;
GO


/* ============================================================
   CUSTOMERS — Basics to Intermediate
   ============================================================ */
-- 1) Recent customers (last 30 days)
SELECT customer_id, name, email, created_at
FROM dbo.customers
WHERE created_at >= DATEADD(DAY, -30, SYSUTCDATETIME())
ORDER BY created_at DESC;

-- 2) Customers without any checkouts (anti-join)
SELECT c.customer_id, c.name, c.email
FROM dbo.customers c
WHERE NOT EXISTS (SELECT 1 FROM dbo.checkouts co WHERE co.customer_id = c.customer_id);

-- 3) Deposit account balances (join accounts)
SELECT c.customer_id, c.name, da.balance_cents
FROM dbo.customers c
LEFT JOIN dbo.deposit_accounts da ON da.customer_id = c.customer_id
ORDER BY da.balance_cents DESC;

-- 4) Running balance from ledger (window SUM)
SELECT
  c.customer_id, c.name, dt.created_at, dt.delta_cents,
  SUM(dt.delta_cents) OVER (PARTITION BY da.account_id ORDER BY dt.created_at
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_balance_cents
FROM dbo.deposit_accounts da
JOIN dbo.customers c ON c.customer_id = da.customer_id
JOIN dbo.deposit_transactions dt ON dt.account_id = da.account_id
ORDER BY c.customer_id, dt.created_at;

-- 5) Top customers by penalties (negative deltas)
SELECT TOP 10 c.customer_id, c.name, SUM(CASE WHEN dt.delta_cents < 0 THEN -dt.delta_cents ELSE 0 END) AS total_penalties
FROM dbo.customers c
JOIN dbo.deposit_accounts da ON da.customer_id = c.customer_id
JOIN dbo.deposit_transactions dt ON dt.account_id = da.account_id
GROUP BY c.customer_id, c.name
ORDER BY total_penalties DESC;
GO


/* ============================================================
   PACKAGING CATALOG & INSTANCES — Basics to Advanced
   ============================================================ */
-- 1) Catalog summary by kind & material
SELECT kind, material, COUNT(*) AS items, AVG(CAST(capacity_ml AS FLOAT)) AS avg_capacity_ml
FROM dbo.packaging_catalog
GROUP BY kind, material
ORDER BY kind, material;

-- 2) Instances count by state
SELECT state, COUNT(*) AS instances
FROM dbo.packaging_instances
GROUP BY state
ORDER BY instances DESC;

-- 3) Instances per catalog (join)
SELECT pc.sku, pc.kind, COUNT(pi.instance_id) AS instances
FROM dbo.packaging_catalog pc
LEFT JOIN dbo.packaging_instances pi ON pi.catalog_id = pc.catalog_id
GROUP BY pc.sku, pc.kind
ORDER BY instances DESC;

-- 4) Last known location for each instance (ROW_NUMBER) + view
CREATE OR ALTER VIEW dbo.v_instance_last_location AS
WITH ranked AS (
  SELECT
    m.instance_id,
    m.to_loc_id,
    m.moved_at,
    ROW_NUMBER() OVER (PARTITION BY m.instance_id ORDER BY m.moved_at DESC, m.mv_id DESC) AS rn
  FROM dbo.movements m
)
SELECT instance_id, to_loc_id, moved_at
FROM ranked
WHERE rn = 1;
GO

-- 5) Instances with retailer name for last known location
SELECT pi.instance_id, pi.uid_code, l.name AS last_location
FROM dbo.packaging_instances pi
LEFT JOIN dbo.v_instance_last_location v ON v.instance_id = pi.instance_id
LEFT JOIN dbo.locations l ON l.location_id = v.to_loc_id
ORDER BY pi.instance_id;

-- 6) Overdue items (no return yet and due date passed)
SELECT c.checkout_id, pi.uid_code,
       DATEADD(DAY, c.due_back_days, c.checkout_time) AS due_date,
       DATEDIFF(DAY, DATEADD(DAY, c.due_back_days, c.checkout_time), SYSUTCDATETIME()) AS days_overdue
FROM dbo.checkouts c
JOIN dbo.packaging_instances pi ON pi.instance_id = c.instance_id
LEFT JOIN dbo.returns r ON r.checkout_id = c.checkout_id
WHERE r.return_id IS NULL
  AND SYSUTCDATETIME() > DATEADD(DAY, c.due_back_days, c.checkout_time)
ORDER BY days_overdue DESC;
GO


/* ============================================================
   CHECKOUTS & RETURNS — Cycle analytics
   ============================================================ */
-- 1) Completed cycle durations (hours) and basic stats
SELECT
  AVG(DATEDIFF(HOUR, c.checkout_time, r.return_time)) AS avg_hours,
  MIN(DATEDIFF(HOUR, c.checkout_time, r.return_time)) AS min_hours,
  MAX(DATEDIFF(HOUR, c.checkout_time, r.return_time)) AS max_hours
FROM dbo.checkouts c
JOIN dbo.returns r ON r.checkout_id = c.checkout_id;

-- 2) Percentile durations by catalog kind
SELECT
  pc.kind,
  PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DATEDIFF(HOUR, c.checkout_time, r.return_time)) AS p50_h,
  PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY DATEDIFF(HOUR, c.checkout_time, r.return_time)) AS p90_h
FROM dbo.checkouts c
JOIN dbo.returns r ON r.checkout_id = c.checkout_id
JOIN dbo.packaging_instances pi ON pi.instance_id = c.instance_id
JOIN dbo.packaging_catalog pc ON pc.catalog_id = pi.catalog_id
GROUP BY pc.kind
ORDER BY pc.kind;

-- 3) Return rate by retailer
SELECT l.name AS retailer, COUNT(r.return_id)*1.0 / COUNT(c.checkout_id) AS return_rate
FROM dbo.checkouts c
JOIN dbo.retailers rr ON rr.retailer_id = c.retailer_id
JOIN dbo.locations l ON l.location_id = rr.location_id
LEFT JOIN dbo.returns r ON r.checkout_id = c.checkout_id
GROUP BY l.name
ORDER BY return_rate DESC;

-- 4) On-time vs late returns (labeling via CASE)
SELECT
  CASE WHEN r.return_id IS NULL THEN 'open'
       WHEN r.return_time <= DATEADD(DAY, c.due_back_days, c.checkout_time) THEN 'on_time'
       ELSE 'late' END AS return_status,
  COUNT(*) AS totals
FROM dbo.checkouts c
LEFT JOIN dbo.returns r ON r.checkout_id = c.checkout_id
GROUP BY CASE WHEN r.return_id IS NULL THEN 'open'
              WHEN r.return_time <= DATEADD(DAY, c.due_back_days, c.checkout_time) THEN 'on_time'
              ELSE 'late' END
ORDER BY totals DESC;
GO


/* ============================================================
   WASH CYCLES & INSPECTIONS — Quality analytics
   ============================================================ */
-- 1) Wash cycle duration and temperature stats
SELECT
  COUNT(*) AS cycles,
  AVG(CASE WHEN end_time IS NOT NULL THEN DATEDIFF(MINUTE, start_time, end_time) END) AS avg_minutes,
  AVG(temp_c) AS avg_temp_c
FROM dbo.wash_cycles;

-- 2) Inspection pass rate per hub
SELECT l.name AS hub_name,
       AVG(CASE WHEN i.result = N'pass' THEN 1.0 ELSE 0.0 END) AS pass_rate
FROM dbo.inspections i
LEFT JOIN dbo.wash_cycles w ON w.wash_id = i.wash_id
LEFT JOIN dbo.hubs h ON h.hub_id = w.hub_id
LEFT JOIN dbo.locations l ON l.location_id = h.location_id
GROUP BY l.name
ORDER BY pass_rate DESC;

-- 3) Latest inspection per instance + failures
WITH latest AS (
  SELECT i.*,
         ROW_NUMBER() OVER (PARTITION BY i.instance_id ORDER BY i.inspected_at DESC, i.inspection_id DESC) AS rn
  FROM dbo.inspections i
)
SELECT instance_id, result, inspected_at, notes
FROM latest
WHERE rn = 1 AND result = N'fail'
ORDER BY inspected_at DESC;
GO


/* ============================================================
   CONTAMINATION INCIDENTS — Safety analytics
   ============================================================ */
-- 1) Incidents by kind & severity (last 30 days)
SELECT kind, severity, COUNT(*) AS incidents_30d
FROM dbo.contamination_incidents
WHERE detected_at >= DATEADD(DAY, -30, SYSUTCDATETIME())
GROUP BY kind, severity
ORDER BY incidents_30d DESC;

-- 2) Instances with repeated incidents (2+)
SELECT instance_id, COUNT(*) AS total_incidents
FROM dbo.contamination_incidents
GROUP BY instance_id
HAVING COUNT(*) >= 2
ORDER BY total_incidents DESC;
GO


/* ============================================================
   SENSOR READINGS — Ops analytics
   ============================================================ */
-- 1) Last reading per (instance_id, sensor_type)
WITH ranked AS (
  SELECT s.*,
         ROW_NUMBER() OVER (PARTITION BY s.instance_id, s.sensor_type ORDER BY s.measured_at DESC, s.reading_id DESC) AS rn
  FROM dbo.sensor_readings s
)
SELECT instance_id, sensor_type, value, measured_at
FROM ranked
WHERE rn = 1
ORDER BY instance_id, sensor_type;

-- 2) Temperature breach (last 24 hours) > 35C
SELECT s.instance_id, MAX(s.value) AS max_temp_24h
FROM dbo.sensor_readings s
WHERE s.sensor_type = N'temperature'
  AND s.measured_at >= DATEADD(HOUR, -24, SYSUTCDATETIME())
GROUP BY s.instance_id
HAVING MAX(s.value) > 35
ORDER BY max_temp_24h DESC;

-- 3) 3-day averages pivot by sensor type
SELECT *
FROM (
  SELECT CAST(measured_at AS DATE) AS [date], sensor_type, value
  FROM dbo.sensor_readings
  WHERE measured_at >= DATEADD(DAY, -3, SYSUTCDATETIME())
) src
PIVOT (AVG(value) FOR sensor_type IN ([temperature],[humidity],[shock])) p
ORDER BY [date];
GO


/* ============================================================
   MOVEMENTS — Logistics analytics
   ============================================================ */
-- 1) Flow counts by from→to kind (retailer/hub/dropbox)
SELECT lf.kind AS from_kind, lt.kind AS to_kind, COUNT(*) AS moves
FROM dbo.movements m
LEFT JOIN dbo.locations lf ON lf.location_id = m.from_loc_id
LEFT JOIN dbo.locations lt ON lt.location_id = m.to_loc_id
GROUP BY lf.kind, lt.kind
ORDER BY moves DESC;

-- 2) Dwell time per instance at last location (LAG-based)
WITH seq AS (
  SELECT m.instance_id, m.to_loc_id, m.moved_at,
         LAG(m.moved_at) OVER (PARTITION BY m.instance_id ORDER BY m.moved_at) AS prev_move
  FROM dbo.movements m
),
last_step AS (
  SELECT instance_id, to_loc_id, moved_at, prev_move,
         ROW_NUMBER() OVER (PARTITION BY instance_id ORDER BY moved_at DESC) AS rn
  FROM seq
)
SELECT ls.instance_id, l.name AS last_location,
       DATEDIFF(HOUR, ls.prev_move, ls.moved_at) AS dwell_hours
FROM last_step ls
LEFT JOIN dbo.locations l ON l.location_id = ls.to_loc_id
WHERE rn = 1
ORDER BY dwell_hours DESC;
GO


/* ============================================================
   AUDIT LOGS — Monitoring
   ============================================================ */
-- 1) Most frequent event types
SELECT event_type, COUNT(*) AS cnt
FROM dbo.audit_logs
GROUP BY event_type
ORDER BY cnt DESC;

-- 2) Latest 20 logs for a given entity type
DECLARE @entity NVARCHAR(30) = N'instance';
SELECT TOP 20 log_id, entity_id, event_type, created_at
FROM dbo.audit_logs
WHERE entity_type = @entity
ORDER BY created_at DESC;

-- 3) If 'detail' stores valid JSON, extract a 'note' (guarded)
SELECT TOP 20
  log_id,
  CASE WHEN ISJSON(detail) = 1 THEN JSON_VALUE(detail, '$.note') ELSE NULL END AS note,
  created_at
FROM dbo.audit_logs
ORDER BY created_at DESC;
GO


/* ============================================================
   KPIs & REPORTING — GROUPING SETS / ROLLUP
   ============================================================ */
-- 1) Daily checkouts and returns
SELECT CAST(c.checkout_time AS DATE) AS [date],
       COUNT(*) AS total_checkouts,
       COUNT(r.return_id) AS total_returns
FROM dbo.checkouts c
LEFT JOIN dbo.returns r ON r.checkout_id = c.checkout_id
GROUP BY CAST(c.checkout_time AS DATE)
ORDER BY [date] DESC;

-- 2) Instances by state with ROLLUP
SELECT state, COUNT(*) AS cnt
FROM dbo.packaging_instances
GROUP BY ROLLUP(state);

-- 3) Retailer-level dashboard aggregates
SELECT
  l.name AS retailer,
  COUNT(DISTINCT c.checkout_id) AS checkouts,
  COUNT(DISTINCT r.return_id)   AS returns,
  SUM(CASE WHEN r.return_id IS NULL THEN 1 ELSE 0 END) AS open_loans
FROM dbo.checkouts c
JOIN dbo.retailers rt ON rt.retailer_id = c.retailer_id
JOIN dbo.locations l ON l.location_id = rt.location_id
LEFT JOIN dbo.returns r ON r.checkout_id = c.checkout_id
GROUP BY l.name
ORDER BY checkouts DESC;
GO


/* ============================================================
   Helpful Views (create once) — Optional
   ============================================================ */
-- A) Enriched checkout view
CREATE OR ALTER VIEW dbo.v_checkout_enriched AS
SELECT
  c.checkout_id, c.checkout_time, c.due_back_days,
  cu.customer_id, cu.name AS customer_name,
  pi.instance_id, pi.uid_code, pi.state,
  pc.kind, pc.material, pc.capacity_ml,
  rl.location_id AS retailer_location_id, rl.name AS retailer_name
FROM dbo.checkouts c
JOIN dbo.customers cu ON cu.customer_id = c.customer_id
JOIN dbo.packaging_instances pi ON pi.instance_id = c.instance_id
JOIN dbo.packaging_catalog pc ON pc.catalog_id = pi.catalog_id
JOIN dbo.retailers r ON r.retailer_id = c.retailer_id
JOIN dbo.locations rl ON rl.location_id = r.location_id;
GO

-- B) Customer balances view (ledger-sum + account)
CREATE OR ALTER VIEW dbo.v_customer_balances AS
SELECT
  c.customer_id, c.name,
  da.balance_cents,
  COALESCE(SUM(dt.delta_cents),0) AS ledger_sum_cents
FROM dbo.customers c
LEFT JOIN dbo.deposit_accounts da ON da.customer_id = c.customer_id
LEFT JOIN dbo.deposit_transactions dt ON dt.account_id = da.account_id
GROUP BY c.customer_id, c.name, da.balance_cents;
