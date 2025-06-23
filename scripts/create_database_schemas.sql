-- File: create_database_schemas.sql

-- ==================================================================================================
-- SCHEMAS UNTUK adventureworks_staging DATABASE (Staging Area untuk data OLTP AdventureWorks)
-- ==================================================================================================

-- Tabel Raw (salinan langsung dari AdventureWorks OLTP)
CREATE TABLE IF NOT EXISTS raw_salesorderdetail (
    salesorderdetailid INT, salesorderid INT, carriertrackingnumber TEXT, orderqty SMALLINT,
    productid INT, specialofferid INT, unitprice NUMERIC, unitpricediscount NUMERIC,
    rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_salesorderheader (
    salesorderid INT PRIMARY KEY, revisionnumber SMALLINT, orderdate TIMESTAMP, duedate TIMESTAMP,
    shipdate TIMESTAMP, status SMALLINT, onlineorderflag BOOLEAN, salesordernumber TEXT,
    purchaseordernumber TEXT, accountnumber TEXT, customerid INT, salespersonid INT,
    territoryid INT, billtoaddressid INT, shiptoaddressid INT, shipmethodid INT,
    creditcardid INT, creditcardapprovalcode TEXT, currencyrateid INT, subtotal NUMERIC,
    taxamt NUMERIC, freight NUMERIC, totaldue NUMERIC, comment TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_product (
    productid INT PRIMARY KEY, name TEXT, productnumber TEXT, makeflag BOOLEAN,
    finishedgoodsflag BOOLEAN, color TEXT, safetystocklevel SMALLINT, reorderpoint SMALLINT,
    standardcost NUMERIC, listprice NUMERIC, size TEXT, sizeunitmeasurecode TEXT,
    weightunitmeasurecode TEXT, weight NUMERIC, daystomanufacture INT, productline TEXT,
    class TEXT, style TEXT, productsubcategoryid INT, productmodelid INT, sellstartdate TIMESTAMP,
    sellenddate TIMESTAMP, discontinueddate TIMESTAMP, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_customer (
    customerid INT PRIMARY KEY, personid INT, storeid INT, territoryid INT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_person (
    businessentityid INT PRIMARY KEY, persontype TEXT, namestyle BOOLEAN, title TEXT,
    firstname TEXT, middlename TEXT, lastname TEXT, suffix TEXT, emailpromotion INT,
    additionalcontactinfo TEXT, demographics TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_productcategory (
    productcategoryid INT PRIMARY KEY, name TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_productsubcategory (
    productsubcategoryid INT PRIMARY KEY, productcategoryid INT, name TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_store (
    businessentityid INT PRIMARY KEY, name TEXT, salespersonid INT, demographics TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_vendor (
    businessentityid INT PRIMARY KEY, accountnumber TEXT, name TEXT, creditrating SMALLINT,
    preferredvendorstatus BOOLEAN, activeflag BOOLEAN, purchasingwebserviceurl TEXT, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw_productvendor (
    productid INT, businessentityid INT, averageleadtime INT, standardprice NUMERIC,
    lastreceiptcost NUMERIC, lastreceiptdate TIMESTAMP, minorderqty INT, maxorderqty INT,
    onorderqty INT, unitmeasurecode TEXT, modifieddate TIMESTAMP,
    PRIMARY KEY (productid, businessentityid)
);

CREATE TABLE IF NOT EXISTS raw_employee (
    businessentityid INT PRIMARY KEY, nationalidnumber TEXT, loginid TEXT, jobtitle TEXT,
    birthdate DATE, maritalstatus TEXT, gendr TEXT, hiredate DATE, salariedflag BOOLEAN,
    vacationhours INT, sickleavehours INT, currentflag BOOLEAN, rowguid UUID, modifieddate TIMESTAMP,
    organizationnode TEXT
);

CREATE TABLE IF NOT EXISTS raw_employeedepartmenthistory (
    businessentityid INT, departmentid INT, shiftid INT, startdate DATE, enddate DATE, modifieddate TIMESTAMP,
    PRIMARY KEY (businessentityid, departmentid, shiftid, startdate)
);

CREATE TABLE IF NOT EXISTS raw_department (
    departmentid INT PRIMARY KEY, name TEXT, groupname TEXT, modifieddate TIMESTAMP
);

-- Tabel Staging Menengah (stg_...) - Jika Anda punya transformasi dari raw_ ke stg_
-- Ini adalah contoh, pastikan sesuai dengan skema Anda yang sebenarnya dari AdventureWorks OLTP
CREATE TABLE IF NOT EXISTS stg_address (
    addressid INT PRIMARY KEY, addressline1 TEXT, addressline2 TEXT, city TEXT,
    stateprovinceid INT, postalcode TEXT, spatiallocation GEOGRAPHY(POINT, 4326), -- Perlu PostGIS!
    rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_businessentityaddress (
    businessentityid INT, addressid INT, addresstypeid INT, rowguid UUID, modifieddate TIMESTAMP,
    PRIMARY KEY (businessentityid, addressid, addresstypeid)
);

CREATE TABLE IF NOT EXISTS stg_countryregion (
    countryregioncode TEXT PRIMARY KEY, name TEXT, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_customer (
    customerid INT PRIMARY KEY, personid INT, storeid INT, territoryid INT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_department (
    departmentid INT PRIMARY KEY, name TEXT, groupname TEXT, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_emailaddress (
    businessentityid INT, emailaddressid INT, emailaddress TEXT, rowguid UUID, modifieddate TIMESTAMP,
    PRIMARY KEY (businessentityid, emailaddressid)
);

CREATE TABLE IF NOT EXISTS stg_employee (
    businessentityid INT PRIMARY KEY, nationalidnumber TEXT, loginid TEXT, jobtitle TEXT,
    birthdate DATE, maritalstatus TEXT, gendr TEXT, hiredate DATE, salariedflag BOOLEAN,
    vacationhours INT, sickleavehours INT, currentflag BOOLEAN, rowguid UUID, modifieddate TIMESTAMP,
    organizationnode TEXT
);

CREATE TABLE IF NOT EXISTS stg_employeedepartmenthistory (
    businessentityid INT, departmentid INT, shiftid INT, startdate DATE, enddate DATE, modifieddate TIMESTAMP,
    PRIMARY KEY (businessentityid, departmentid, shiftid, startdate)
);

CREATE TABLE IF NOT EXISTS stg_person (
    businessentityid INT PRIMARY KEY, persontype TEXT, namestyle BOOLEAN, title TEXT,
    firstname TEXT, middlename TEXT, lastname TEXT, suffix TEXT, emailpromotion INT,
    additionalcontactinfo TEXT, demographics TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_personphone (
    businessentityid INT, phonenumber TEXT, phonenumbertypeid INT, modifieddate TIMESTAMP,
    PRIMARY KEY (businessentityid, phonenumber, phonenumbertypeid)
);

CREATE TABLE IF NOT EXISTS stg_product (
    productid INT PRIMARY KEY, name TEXT, productnumber TEXT, makeflag BOOLEAN,
    finishedgoodsflag BOOLEAN, color TEXT, safetystocklevel SMALLINT, reorderpoint SMALLINT,
    standardcost NUMERIC, listprice NUMERIC, size TEXT, sizeunitmeasurecode TEXT,
    weightunitmeasurecode TEXT, weight NUMERIC, daystomanufacture INT, productline TEXT,
    class TEXT, style TEXT, productsubcategoryid INT, productmodelid INT, sellstartdate TIMESTAMP,
    sellenddate TIMESTAMP, discontinueddate TIMESTAMP, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_productcategory (
    productcategoryid INT PRIMARY KEY, name TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_productsubcategory (
    productsubcategoryid INT PRIMARY KEY, productcategoryid INT, name TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_productvendor (
    productid INT, businessentityid INT, averageleadtime INT, standardprice NUMERIC,
    lastreceiptcost NUMERIC, lastreceiptdate TIMESTAMP, minorderqty INT, maxorderqty INT,
    onorderqty INT, unitmeasurecode TEXT, modifieddate TIMESTAMP,
    PRIMARY KEY (productid, businessentityid)
);

CREATE TABLE IF NOT EXISTS stg_salesorderdetail (
    salesorderid INT, salesorderdetailid INT, carriertrackingnumber TEXT, orderqty SMALLINT,
    productid INT, specialofferid INT, unitprice NUMERIC, unitpricediscount NUMERIC,
    rowguid UUID, modifieddate TIMESTAMP,
    PRIMARY KEY (salesorderid, salesorderdetailid)
);

CREATE TABLE IF NOT EXISTS stg_salesorderheader (
    salesorderid INT PRIMARY KEY, revisionnumber SMALLINT, orderdate TIMESTAMP, duedate TIMESTAMP,
    shipdate TIMESTAMP, status SMALLINT, onlineorderflag BOOLEAN, salesordernumber TEXT,
    purchaseordernumber TEXT, accountnumber TEXT, customerid INT, salespersonid INT,
    territoryid INT, billtoaddressid INT, shiptoaddressid INT, shipmethodid INT,
    creditcardid INT, creditcardapprovalcode TEXT, currencyrateid INT, subtotal NUMERIC,
    taxamt NUMERIC, freight NUMERIC, totaldue NUMERIC, comment TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_stateprovince (
    stateprovinceid INT PRIMARY KEY, stateprovincecode TEXT, countryregioncode TEXT, isonlystateprovinceflag BOOLEAN,
    name TEXT, territoryid INT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_store (
    businessentityid INT PRIMARY KEY, name TEXT, salespersonid INT, demographics TEXT, rowguid UUID, modifieddate TIMESTAMP
);

CREATE TABLE IF NOT EXISTS stg_vendor (
    businessentityid INT PRIMARY KEY, accountnumber TEXT, name TEXT, creditrating SMALLINT,
    preferredvendorstatus BOOLEAN, activeflag BOOLEAN, purchasingwebserviceurl TEXT, modifieddate TIMESTAMP
);


-- ==================================================================================================
-- SCHEMAS UNTUK adventureworks_dw DATABASE (Data Warehouse)
-- ==================================================================================================

-- Dimensi Umum (bisa dipakai oleh kedua domain: AdventureWorks dan Data Lake)
CREATE TABLE IF NOT EXISTS dim_date (
    datekey INT PRIMARY KEY,
    fulldate DATE,
    day INT,
    month INT,
    year INT
);

-- Star Schema untuk Domain Penjualan (dari AdventureWorks OLTP)
CREATE TABLE IF NOT EXISTS dim_product (
    productid INT PRIMARY KEY,
    name TEXT, color TEXT, size TEXT, weight NUMERIC
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customerid INT PRIMARY KEY,
    name TEXT, title TEXT, demographic TEXT
);

CREATE TABLE IF NOT EXISTS dim_store (
    storeid INT PRIMARY KEY,
    storename TEXT
);

CREATE TABLE IF NOT EXISTS dim_vendor (
    vendorid INT PRIMARY KEY,
    vendorname TEXT
);

CREATE TABLE IF NOT EXISTS dim_employee (
    employeeid INT PRIMARY KEY,
    fullname TEXT,
    jobtitle TEXT,
    department TEXT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    factid SERIAL PRIMARY KEY,
    productid INT REFERENCES dim_product(productid),
    customerid INT REFERENCES dim_customer(customerid),
    storeid INT REFERENCES dim_store(storeid),
    vendorid INT REFERENCES dim_vendor(vendorid),
    employeeid INT REFERENCES dim_employee(employeeid),
    datekey INT REFERENCES dim_date(datekey),
    qtyproduct INT,
    unitprice NUMERIC,
    unitpricedisc NUMERIC,
    totalpenjualan NUMERIC
);

-- Star Schema untuk Domain Data Lake (Sensor Gudang & Social Media)
CREATE TABLE IF NOT EXISTS dim_warehouse_zone (
    zone_id SERIAL PRIMARY KEY,
    zone_name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_sentiment_category (
    sentiment_id SERIAL PRIMARY KEY,
    category_name TEXT UNIQUE
);
-- Insert data master untuk kategori sentimen
INSERT INTO dim_sentiment_category (category_name) VALUES ('Positive') ON CONFLICT (category_name) DO NOTHING;
INSERT INTO dim_sentiment_category (category_name) VALUES ('Negative') ON CONFLICT (category_name) DO NOTHING;
INSERT INTO dim_sentiment_category (category_name) VALUES ('Neutral') ON CONFLICT (category_name) DO NOTHING;


CREATE TABLE IF NOT EXISTS fact_warehouse_temperature (
    fact_temp_id SERIAL PRIMARY KEY,
    datekey INT REFERENCES dim_date(datekey),
    zone_id INT REFERENCES dim_warehouse_zone(zone_id),
    avg_temperature_c NUMERIC,
    avg_humidity_percent NUMERIC
);

CREATE TABLE IF NOT EXISTS fact_social_media_sentiment (
    fact_sentiment_id SERIAL PRIMARY KEY,
    datekey INT REFERENCES dim_date(datekey),
    sentiment_id INT REFERENCES dim_sentiment_category(sentiment_id),
    tweet_count INT,
    avg_sentiment_score NUMERIC,
    top_words_json TEXT
);


-- --- NEW: Dimensi dan Fakta untuk Data Finansial ---
CREATE TABLE IF NOT EXISTS dim_company (
    company_id SERIAL PRIMARY KEY,
    company_name TEXT UNIQUE,
    industry TEXT, -- Contoh kolom tambahan
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS fact_financial (
    fact_financial_id SERIAL PRIMARY KEY,
    datekey INT REFERENCES dim_date(datekey),
    company_id INT REFERENCES dim_company(company_id),
    revenue NUMERIC, -- Contoh metrik yang diekstrak
    net_profit NUMERIC, -- Contoh metrik lain (jika bisa diekstrak)
    report_type TEXT -- Contoh: 'Quarterly', 'Annual'
);
