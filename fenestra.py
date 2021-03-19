from google.cloud import storage
import gzip
import os
import glob
import csv
import datetime

from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Table, MetaData, Column,
    BigInteger, Float, Integer, String,
    DateTime, Numeric, Boolean, UniqueConstraint
)
from sqlalchemy.exc import IntegrityError
from tqdm import tqdm

from sqlalchemy.sql import text

# loads environemt variables stored in .env
# needed for db connection
load_dotenv()


def download_files(blobs, dest_dir="files"):
    os.makedirs(dest_dir, exist_ok=True)
    for blob in blobs:
        filename = os.path.join(dest_dir, blob.name)
        print(f"Downloading file {filename}")
        blob.download_to_filename(filename)


def gunzip_files(dest_dir="files"):
    # Glob to get all filenames ending in `.gz`
    gzip_files = glob.iglob(os.path.join("files", "*.gz"))

    for file_ in gzip_files:
        # "foobar.csv.gz".rpartiton(".") -> ("foobar.csv", ".", "gz")
        gunzip_file = file_.rpartition(".")[0]

        # Open gzip file in read-mode, open output gunzip file in write mode
        with gzip.open(file_) as f_in, open(gunzip_file, "wb") as f_out:
            # Iterate over the input file lines
            for line in f_in:
                # write the input file's line to the output file
                f_out.write(line)


def get_connection(echo=False):
    
    try:
        db_user = os.environ["DB_USER"]
    except KeyError:
        raise Exception("No `DB_USER` env variable exists") from None

    try:
        db_password = os.environ["DB_PASSWORD"]
    except KeyError:
        raise Exception("No `DB_PASSWORD` env variable exists") from None

    try:
        db_host = os.environ["DB_HOST"]
    except KeyError:
        raise Exception("No `DB_HOST` env variable exists") from None

    try:
        db_name = os.environ["DB_NAME"]
    except KeyError:
        raise Exception("No `DB_NAME` env variable exists") from None

    conn_str = f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_name}"
    engine = create_engine(conn_str, echo=echo)
    conn = engine.connect()

    return conn


def create_table(conn, meta, table_name="test"):

    conn.execute(f"DROP TABLE IF EXISTS {table_name};")

    table = Table(
        table_name,
        meta,
        Column("id", BigInteger, primary_key=True), 
        Column("Time".casefold(), DateTime, nullable=True), # nullable=True - if value isn't allowed (eg: string for int), make it null 
        Column("AdvertiserId".casefold(), Numeric, nullable=True),
        Column("OrderId".casefold(), Numeric, nullable=True),
        Column("LineItemId".casefold(), Numeric, nullable=True),
        Column("CreativeId".casefold(), Numeric, nullable=True),
        Column("CreativeVersion".casefold(), String, nullable=True),
        Column("CreativeSize".casefold(), String(9), nullable=True),
        Column("AdUnitId".casefold(), String, nullable=True),
        Column("Domain".casefold(), String, nullable=True),
        Column("CountryId".casefold(), Numeric, nullable=True),
        Column("RegionID".casefold(), Numeric, nullable=True),
        Column("MetroId".casefold(), Numeric, nullable=True),
        Column("CityId".casefold(), Numeric, nullable=True),
        Column("BrowserId".casefold(), Numeric, nullable=True),
        Column("OSId".casefold(), Numeric, nullable=True),
        Column("OSVersion".casefold(), String(256), nullable=True),
        Column("TimeUsec2".casefold(), Numeric, nullable=True),
        Column("KeyPart".casefold(), String(9), nullable=True),
        Column("Product".casefold(), String, nullable=True),
        Column("RequestedAdUnitSizes".casefold(), String, nullable=True),
        Column("BandwidthGroupId".casefold(), Numeric, nullable=True),
        Column("MobileDevice".casefold(), String(256), nullable=True),
        Column("IsCompanion".casefold(), Boolean, nullable=True),
        Column("DeviceCategory".casefold(), String, nullable=True),
        Column("ActiveViewEligibleImpression".casefold(), String, nullable=True),
        Column("MobileCarrier".casefold(), String(256), nullable=True),
        Column("EstimatedBackfillRevenue".casefold(), Numeric, nullable=True),
        Column("GfpContentId".casefold(), Numeric, nullable=True),
        Column("PostalCodeId".casefold(), Numeric, nullable=True),
        Column("BandwidthId".casefold(), Numeric, nullable=True),
        Column("AudienceSegmentIds".casefold(), String, nullable=True),
        Column("MobileCapability".casefold(), String(256), nullable=True),
        Column("PublisherProvidedID".casefold(), String(64), nullable=True),
        Column("VideoPosition".casefold(), Numeric, nullable=True),
        Column("PodPosition".casefold(), Numeric, nullable=True),
        Column("VideoFallbackPosition".casefold(), Numeric, nullable=True),
        Column("IsInterstitial".casefold(), Boolean, nullable=True),
        Column("EventTimeUsec2".casefold(), Numeric, nullable=True),
        Column("EventKeyPart".casefold(), String(9), nullable=True),
        Column("YieldGroupCompanyId".casefold(), Numeric, nullable=True),
        Column("RequestLanguage".casefold(), String, nullable=True),
        Column("DealId".casefold(), String, nullable=True),
        Column("SellerReservePrice".casefold(), Float, nullable=True),
        Column("DealType".casefold(), String, nullable=True),
        Column("AdxAccountId".casefold(), Numeric, nullable=True),
        Column("Buyer".casefold(), String, nullable=True),
        Column("Advertiser".casefold(), String, nullable=True),
        Column("Anonymous".casefold(), Boolean, nullable=True),
        Column("ImpressionId".casefold(), String, nullable=True),
        UniqueConstraint("AdvertiserId".casefold(), "OrderId".casefold(), "LineItemId".casefold(), "CreativeId".casefold())
    )

    meta.create_all(conn.engine)


def insert_rows(conn, dest_dir="files", table_name="test"):

    insert_query = text(
        f"INSERT INTO {table_name} (Time,AdvertiserId,OrderId,LineItemId,CreativeId,CreativeVersion,CreativeSize,AdUnitId,Domain,CountryId,RegionId,MetroId,CityId,BrowserId,OSId,OSVersion,TimeUsec2,KeyPart,Product,RequestedAdUnitSizes,BandwidthGroupId,MobileDevice,IsCompanion,DeviceCategory,ActiveViewEligibleImpression,MobileCarrier,EstimatedBackfillRevenue,GfpContentId,PostalCodeId,BandwidthId,AudienceSegmentIds,MobileCapability,PublisherProvidedID,VideoPosition,PodPosition,VideoFallbackPosition,IsInterstitial,EventTimeUsec2,EventKeyPart,YieldGroupCompanyId,RequestLanguage,DealId,SellerReservePrice,DealType,AdxAccountId,Buyer,Advertiser,Anonymous,ImpressionId) "
        "VALUES (:Time,:AdvertiserId,:OrderId,:LineItemId,:CreativeId,:CreativeVersion,:CreativeSize,:AdUnitId,:Domain,:CountryId,:RegionId,:MetroId,:CityId,:BrowserId,:OSId,:OSVersion,:TimeUsec2,:KeyPart,:Product,:RequestedAdUnitSizes,:BandwidthGroupId,:MobileDevice,:IsCompanion,:DeviceCategory,:ActiveViewEligibleImpression,:MobileCarrier,:EstimatedBackfillRevenue,:GfpContentId,:PostalCodeId,:BandwidthId,:AudienceSegmentIds,:MobileCapability,:PublisherProvidedID,:VideoPosition,:PodPosition,:VideoFallbackPosition,:IsInterstitial,:EventTimeUsec2,:EventKeyPart,:YieldGroupCompanyId,:RequestLanguage,:DealId,:SellerReservePrice,:DealType,:AdxAccountId,:Buyer,:Advertiser,:Anonymous,:ImpressionId);"
    )

    csv_files = glob.iglob(os.path.join(dest_dir, "*.csv"))

    duplicate_rows = 0

    for csv_file in csv_files:
        
        with open(csv_file) as f:
            # DictReader (instead of using csv.reader) converts each row as a dict with keys being the header columns and values being the values on each row
            rows = csv.DictReader(f)

            # sanitising the data
            for idx, row in enumerate(tqdm(rows)):
                # if condition below should be applied for to use less data
                # if idx == 1000:
                #     break
                row_modified = {}
                for key, value in row.items():
                    value = value.strip()
                    if value == "":
                        value = None
                    elif value == "true":
                        value = True
                    elif value == "false":
                        value = False
                    row_modified[key] = value
                row_modified['Time'] = datetime.datetime.strptime(row['Time'], '%Y-%m-%d-%H:%M:%S')

                try:
                    conn.execute(insert_query, **row_modified) # bar = {"foo": 10} -> **bar (foo=10)  # splat operator (dict unpacking)
                except IntegrityError:
                    duplicate_rows += 1
                    
    return duplicate_rows


# How many records are there per day and per hour?
# What is the total of the EstimatedBackFillRevenue field per day and per hour?
def run_queries_per_day_per_hour(conn):
    query = '''
    SELECT 
        date(time), extract(hour from time),
        COUNT(*),
        SUM(estimatedbackfillrevenue) 
    FROM test2
    GROUP BY date(time), extract(hour from time)
    '''
    result = conn.execute(query)
    for row in result:
        print(row)

# How many records and what is the total of the EstimatedBackFillRevenue
# field per Buyer?
def run_queries_per_buyer(conn):
    query = '''
    SELECT 
        buyer,
        COUNT(*),
        SUM(estimatedbackfillrevenue) 
    FROM test2
    GROUP BY buyer
    '''
    result = conn.execute(query)
    for row in result:
        print(row)

# List the unique Device Categories by Advertiser
def run_queries_per_advertiser(conn):
    query = '''
    SELECT DISTINCT
        advertiser,
        devicecategory
    FROM test2
    '''
    result = conn.execute(query)
    for row in result:
        print(row)


if __name__ == "__main__":
    storage_client = storage.Client()
    bucket = storage_client.bucket("fst-python-case-study")
    blobs = list(storage_client.list_blobs(bucket))
    download_files(blobs)
    gunzip_files()
    conn = get_connection()
    meta = MetaData()

    try:
        create_table(conn, meta, table_name='test2')
        duplicate_rows_count = insert_rows(conn, table_name='test2')
        print(duplicate_rows_count)
        run_queries_per_day_per_hour(conn)
        run_queries_per_buyer(conn)
        run_queries_per_advertiser(conn)
    finally:
        conn.close()
