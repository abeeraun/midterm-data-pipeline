# Hybrid Order Data Pipeline

مشروع عملي لمقرر **Big Data** لبناء Hybrid Data Pipeline لمعالجة بيانات
طلبات غير نظيفة باستخدام **Python Batch وPySpark وMongoDB**، مع تنفيذ
الاستعلامات والتحليلات وMaterialized Views والمهام المجدولة وواجهة
FastAPI.

يطبق المشروع نمط **ELT**؛ إذ يتم تحميل السجلات الخام أولًا إلى MongoDB،
ثم تطبيق Data Cleaning وData Quality Rules، وبعد ذلك تصنيف السجلات إلى
**Valid / Corrected / Quarantine**.

## 1. Requirements

-   Python 3.10+
-   Java 17+ لتشغيل PySpark
-   MongoDB
-   مساحة تخزين كافية عند تشغيل الملف الكبير

يجب تشغيل MongoDB قبل بدء الـ Pipeline.

## 2. Installation

### Windows

``` powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Linux / macOS

``` bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## 3. Configuration

الإعدادات الأساسية موجودة في `config/settings.py`، ويمكن استخدام
Environment Variables للقيم التي تختلف بين الأجهزة.

أهم الإعدادات:

``` text
SMALL_FILE_THRESHOLD_MB = 200
BATCH_SIZE = 1000
MONGO_BATCH_SIZE = 500
SPARK_MASTER = local[2]
SPARK_SHUFFLE_PARTITIONS = 200
COLLECTION_RAW = orders_raw
COLLECTION_VALIDATED = orders_validated
COLLECTION_QUARANTINE = orders_quarantine
```

يوجد أيضًا `example.env` كنموذج للقيم المطلوبة.

يختار File Router **Python Batch** عندما يكون حجم الملف أقل من أو يساوي
200 MB، ويختار **PySpark** عندما يتجاوز الملف هذا الحد.

## 4. Running the Pipeline

نقطة التشغيل الرئيسية هي `src.main`. يقرأ البرنامج مسار الملف وحجمه،
وينشئ `run_id`، ثم يختار Processing Engine المناسب.

### Python Batch

``` powershell
python -m src.main --input data\orders_sample.csv --threshold-mb 100
```

### PySpark

``` powershell
python -m src.main --input data\orders_huge_mixed_quality.csv --threshold-mb 0
```

استخدام `threshold-mb 0` يجبر الـ Router على اختيار PySpark للاختبار.

## 5. Creating a Test Sample

يمكن إنشاء Sample من الملف الكبير دون Excel أو تعديل البيانات يدويًا:

``` powershell
python -m src.create_small_sample --input data\orders_huge_mixed_quality.csv --output data\orders_sample_500.csv --rows 500 --seed 42
```

قيمة `rows` قابلة للتغيير.

## 6. Pipeline Architecture

تبدأ العملية بقراءة مسار الملف وحجمه وإنشاء `run_id`، ثم يختار File
Router المحرك المناسب.

يتم تحميل جميع السجلات إلى `orders_raw` قبل تطبيق Cleaning أو Quality
Filtering. بعد ذلك تطبق قواعد التحويل والتحقق، ثم يصنف كل سجل إلى Valid
أو Corrected أو Quarantine.

تتم كتابة Valid وCorrected إلى `orders_validated` باستخدام **Idempotent
Upsert**، بينما تتم كتابة السجلات غير القابلة للتصحيح إلى
`orders_quarantine` مع سبب العزل والسجل الخام.

تحفظ Metrics الخاصة بكل Run في `reports/results.json`.

## 7. Python Batch Loader

يستخدم Python Batch Loader قراءة Streaming من خلال `csv` module، ويعالج
السجلات في Batches قابلة للضبط بدل تحميل الملف كاملًا إلى الذاكرة.

يتم تسجيل عدد السجلات والزمن وThroughput لكل Batch، مع استخدام
`insert_many` للـ Raw وBulk Upsert للـ Validated.

## 8. PySpark Loader

يستخدم PySpark Loader:

-   `SparkSession`
-   DataFrame API
-   Fixed Schema بدل `inferSchema`
-   قراءة الحقول الحساسة كـ String في Raw للمحافظة على القيم الأصلية
-   PySpark MongoDB Connector للكتابة إلى MongoDB

يسجل Input Partitions والزمن وThroughput. وتم ضبط
`SPARK_SHUFFLE_PARTITIONS` على 200.

## 9. Data Quality Rules

يطبق المشروع أكثر من ثماني قواعد تنظيف وتصحيح، من أهمها:

-   تحويل Arabic وPersian Digits
-   توحيد Currency
-   معالجة Thousand Separators
-   تحويل الأسعار المكتوبة بالكلمات
-   تطبيع Phone Number
-   تصحيح Repeated Email Symbols
-   توحيد Date Format
-   إزالة المسافات وتوحيد المرادفات
-   إعادة حساب Order Total عند صلاحية مكوناته

لا يتم التصحيح إلا عندما تكون قاعدة التحويل واضحة. أما القيم التي لا
يمكن تصحيحها بأمان فتنتقل إلى Quarantine.

كل سجل Corrected يحتفظ بـ Audit Trail داخل `corrections`، ويشمل Field
وOriginal Value وCorrected Value وRule Code.

## 10. Quarantine

تحتوي `orders_quarantine` على السجلات التي لا يمكن تصحيحها بأمان.

يتضمن كل مستند:

-   Run ID
-   Source File
-   Source Row Number
-   Error Codes
-   Error Details
-   Raw Record

ومن أمثلة أسباب العزل:

-   Missing Order ID
-   Missing Customer ID
-   Invalid or Impossible Date
-   Corrupted Items JSON
-   Empty Items
-   Unknown Price
-   Ambiguous Negative Value
-   Duplicate Order ID
-   Multiple Conflicting Errors

## 11. MongoDB Collections

### `orders_raw`

السجلات الأصلية كما وصلت، مع بيانات المصدر والتشغيل.

### `orders_validated`

السجلات Valid وCorrected القابلة للاستخدام، مع Quality Status
وCorrections وRecord Hash.

### `orders_quarantine`

السجلات التي لم يمكن تصحيحها، مع أسباب العزل والسجل الخام.

يحتوي `orders_validated` على Unique Index باسم `uniq_order_id` على
`order_id`، ويستخدم `order_id` كـ Stable Business Key في Upsert.

كما توجد Indexes إضافية للاستعلامات والتحليلات:

-   `idx_customer_id`
-   `idx_city`
-   `idx_status_order_date` --- Compound Index

## 12. Idempotency and Upsert

تم اختبار إعادة تشغيل نفس Sample في Python Batch وPySpark.

في التشغيل الثاني لم يزد عدد المستندات في `orders_validated` ولم تظهر
Business Records مكررة.

تم أيضًا تعديل سجل موجود ثم إعادة تشغيل Pipeline، وتم التحقق من تنفيذ
Update بدل إنشاء سجل جديد.

## 13. Large-Scale Run

تم تشغيل الملف الكبير باستخدام PySpark بنجاح.

النتائج المسجلة:

  Metric                                       Result
  ------------------------- -------------------------
  Engine                                      PySpark
  Rows Read                                30,000,000
  Loaded Raw                               30,000,000
  Valid                                    22,343,466
  Corrected                                 5,697,571
  Quarantine                                1,958,963
  Input Partitions                                 99
  Processing Time                   19,913.98 seconds
  Throughput                  1,506.48 records/second
  Inserted into Validated                  28,041,037

Consistency Check:

``` text
30,000,000 = 22,343,466 + 5,697,571 + 1,958,963
```

وكذلك:

``` text
28,041,037 = 22,343,466 + 5,697,571
```

## 14. Queries and Indexes

يتضمن الجزء النهائي خمسة استعلامات عملية على `orders_validated`:

1.  Customer ID
2.  City
3.  Status + Date Range
4.  Order ID
5.  Date Range

الاستعلامات موجودة داخل:

``` text
src/queries/
```

والملفات هي:

``` text
query_01_customer.py
query_02_city.py
query_03_status_date.py
query_04_order_id.py
query_05_date_range.py
```

### Final Indexes

``` text
uniq_order_id
idx_customer_id
idx_city
idx_status_order_date
```

تم استخدام `executionStats` لمقارنة الأداء قبل وبعد إنشاء Indexes.

الـ Compound Index هو:

``` text
(status, order_date)
```

ويستخدم مع الاستعلام الذي يجمع بين Status وDate Range.

## 15. Aggregations

يتضمن المشروع خمسة تقارير Aggregation مستقلة، وكل تقرير يعيد بيانات
فعلية من MongoDB.

الموجودة داخل:

``` text
src/aggregations/
```

-   `aggregation_01_city_sales.py` --- Sales حسب City
-   `aggregation_02_status_sales.py` --- Sales حسب Status
-   `aggregation_03_daily_sales.py` --- Daily Sales
-   `aggregation_04_top_customers.py` --- Top Customers
-   `aggregation_05_city_status.py` --- City وStatus Analysis

يسمح تقرير Top Customers بتحديد Date Range قبل تنفيذ التجميع.

## 16. Materialized Views

يتضمن المشروع Materialized Views مبنية على نتائج Aggregations.

الموجودة داخل:

``` text
src/materialized_views/
```

-   `mv_01_city_sales.py` → `mv_city_sales`
-   `mv_02_daily_sales.py` → `mv_daily_sales`

يدعم كل View:

-   Full Refresh
-   Incremental Refresh

تم اختبار آليات Full Refresh وIncremental Refresh لكل من City Sales
وDaily Sales.

## 17. Scheduled Jobs

يتضمن المشروع وظيفتين لتحديث Materialized Views:

-   `refresh_city_sales`
-   `refresh_daily_sales`

الوظائف موجودة في:

``` text
src/jobs/job_runner.py
```

كل Job يسجل:

-   Start Time
-   Finish Time
-   Status
-   Result أو Error

كما يوجد Scheduler باستخدام APScheduler في:

``` text
src/scheduler.py
```

ويتم تسجيل وظيفتي التحديث بجدول زمني كل ساعة.

## 18. FastAPI

تمت إضافة واجهة FastAPI موحدة للوصول إلى وظائف المشروع الموجودة بدل
إنشاء Pipeline منفصل.

الملف:

``` text
src/api.py
```

### Endpoints

  Method   Endpoint
  -------- ------------------------
  GET      `/health`
  POST     `/ingest`
  POST     `/indexes`
  GET      `/queries`
  GET      `/queries/{name}`
  GET      `/aggregations`
  GET      `/aggregations/{name}`
  POST     `/refresh-mv`
  GET      `/jobs`
  POST     `/jobs/{name}/run`

تشغل API باستخدام:

``` powershell
uvicorn src.api:app --reload
```

ثم يمكن فتح Swagger من:

``` text
http://127.0.0.1:8000/docs
```

يستخدم `/ingest` نفس Router وPipeline المستخدمين في التشغيل الأساسي
للمشروع.

## 19. Reports and Metrics

تحفظ Metrics في:

``` text
reports/results.json
```

وتتضمن Run ID واسم الملف وحجمه وProcessing Engine وعدد السجلات المقروءة
والسجلات المحملة إلى Raw وأعداد Valid وCorrected وQuarantine والزمن
وThroughput وBatch Size أو Partitions وعدادات Inserted وUpdated
وUnchanged وConsistency Check.

يوجد أيضًا:

``` text
reports/results.md
reports/final_evidence.md
docs/architecture.md
```

## 20. Testing

لتشغيل الاختبارات:

``` powershell
python -m pytest -q
```

آخر نتيجة مسجلة:

``` text
25 passed
```

وتغطي الاختبارات قواعد Cleaning وClassification الأساسية، إضافة إلى
اختبارات يدوية لـ Classification Parity وIdempotency وDuplicate
Prevention وUpdate في Python Batch وPySpark.

## 21. Resource Management

يستخدم المشروع `try/finally` لإغلاق SparkSession واتصال MongoDB حتى عند
حدوث خطأ أثناء التشغيل.

## 22. Project Structure

``` text
midterm-data-pipeline/
├── README.md
├── requirements.txt
├── example.env
├── config/
│   ├── __init__.py
│   └── settings.py
├── data/
│   └── .gitkeep
├── docs/
│   └── architecture.md
├── reports/
│   ├── results.md
│   ├── results.json
│   ├── final_evidence.md
│   └── screenshots/
├── src/
│   ├── __init__.py
│   ├── api.py
│   ├── main.py
│   ├── file_router.py
│   ├── create_small_sample.py
│   ├── batch_loader.py
│   ├── spark_loader.py
│   ├── quality_rules.py
│   ├── elt_pipeline.py
│   ├── incremental_loader.py
│   ├── mongo_setup.py
│   ├── metrics.py
│   ├── final_indexes.py
│   ├── scheduler.py
│   ├── aggregations/
│   │   ├── aggregation_01_city_sales.py
│   │   ├── aggregation_02_status_sales.py
│   │   ├── aggregation_03_daily_sales.py
│   │   └── aggregation_04_top_customers.py
│   ├── jobs/
│   │   ├── __init__.py
│   │   └── job_runner.py
│   ├── materialized_views/
│   │   ├── mv_01_city_sales.py
│   │   └── mv_02_daily_sales.py
│   └── queries/
│       ├── query_01_customer.py
│       ├── query_02_city.py
│       ├── query_03_status_date.py
│       ├── query_04_order_id.py
│       └── query_05_date_range.py
└── tests/
    ├── test_cleaning_rules.py
    └── test_classification.py
```

## 23. GitHub

المشروع:

https://github.com/abeeraun/midterm-data-pipeline
