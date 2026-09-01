Architecture

1. فكرة المشروع

المشروع عبارة عن Hybrid Data Pipeline لمعالجة ملف CSV يحتوي على بيانات طلبات غير نظيفة. يختار النظام طريقة المعالجة حسب حجم الملف، ثم يطبق قواعد Data Quality ويحفظ النتائج في MongoDB.

يستخدم المشروع نمط ELT. معنى ذلك أن السجلات تصل أولًا إلى Raw Layer كما وردت من المصدر، وبعد ذلك تبدأ عملية التنظيف والتصنيف.

2. المكونات الرئيسية

يحتوي المشروع على المكونات التالية:

المكون
الوظيفة
src/main.py
نقطة التشغيل الرئيسية وإدارة دورة التنفيذ
src/file_router.py
اختيار Processing Engine حسب حجم الملف
src/batch_loader.py
معالجة الملفات الصغيرة باستخدام Python Batch
src/spark_loader.py
معالجة الملفات الكبيرة باستخدام PySpark
src/quality_rules.py
قواعد التنظيف والتصنيف في مسار Python
src/elt_pipeline.py
تجهيز Raw Documents وValidated Upsert وQuarantine Documents
src/mongo_setup.py
إنشاء Collections وIndexes وSchema Validation
src/metrics.py
حفظ Metrics الخاصة بكل Run
MongoDB
تخزين Raw وValidated وQuarantine




3. تدفق البيانات

mermaid

Source



يحفظ المساران السجل في orders_raw قبل تطبيق Quality Rules. بعد ذلك تذهب السجلات Valid وCorrected إلى orders_validated، بينما تذهب السجلات التي لا يمكن تصحيحها بأمان إلى orders_quarantine.

4. File Router

يبدأ التنفيذ من src/main.py. يقرأ البرنامج مسار الملف وحجمه وينشئ Run ID. بعد ذلك يستدعي File Router.

قيمة الحد الفاصل المستخدمة في المشروع هي 200 MB. إذا كان حجم الملف أقل من أو يساوي هذا الحد، يتم اختيار Python Batch. وإذا تجاوز الملف هذا الحد، يتم اختيار PySpark.

يطبع Router مسار الملف وحجمه وProcessing Engine والسبب الذي أدى إلى الاختيار.

5. Python Batch Path

يستخدم batch_loader.py قراءة Streaming من خلال csv module. تتم قراءة السجلات صفًا صفًا وتجميعها داخل Batches قابلة للضبط. لا يستخدم المسار list(reader)، ولذلك لا يحتاج إلى تحميل الملف كاملًا في الذاكرة.

لكل Batch يقوم البرنامج بما يلي:

يضيف كل سجل إلى orders_raw مع Run ID وSource File وSource Row Number ووقت الإدخال واسم المحرك وRaw Record.

يمرر السجل إلى elt_pipeline.py، الذي يستدعي quality_rules.py لتطبيق قواعد التنظيف والتصنيف.

يجهز Bulk Upsert للسجلات Valid وCorrected، ويجهز Quarantine Document للسجلات غير القابلة للتصحيح.

يكتب Batch إلى MongoDB، ثم يسجل عدد السجلات والزمن وThroughput وعدادات Inserted وUpdated وUnchanged.

6. PySpark Path

يستخدم spark_loader.py SparkSession وDataFrame API لمعالجة الملفات الكبيرة. تتم قراءة الملف باستخدام Schema ثابتة بدل inferSchema، وذلك للمحافظة على القيم غير النظيفة في Raw قبل تطبيق التحويلات.

يقسم Spark البيانات إلى Input Partitions ويطبق Transformations باستخدام DataFrame Expressions. تتم معالجة قواعد التنظيف داخل مسار Spark بصياغة مكافئة لقواعد Python. تم اختبار المسارين على نفس العينة، وأصبح تصنيفهما متطابقًا.

يستخدم المسار MongoDB Spark Connector للكتابة إلى MongoDB. كما يحسب Input Partitions ووقت التنفيذ وThroughput وعدادات Valid وCorrected وQuarantine وInserted وUpdated وUnchanged.

تم ضبط SPARK_SHUFFLE_PARTITIONS على 200 لتقليل الضغط على الذاكرة أثناء عمليات Shuffle للملفات الكبيرة. كما يتم استخدام مجلد مؤقت منفصل لملفات Spark.

7. ELT وRaw Layer

تصل جميع السجلات أولًا إلى orders_raw دون تطبيق Quality Filtering. يحتوي كل Raw Document على الحقول التالية:

الحقل
الغرض
run_id
معرف فريد للتشغيل
source_file
مسار أو اسم الملف المصدر
source_row_number
رقم الصف عندما يكون متاحًا
ingested_at
وقت تحميل السجل
engine_used
Python Batch أو PySpark
raw_record
السجل كما وصل من CSV




تحتفظ orders_raw بتاريخ التشغيلات المختلفة. لذلك يمكن أن يحتوي هذا Collection على أكثر من نسخة من السجل نفسه عند إعادة تشغيل الملف، وتستخدم run_id للتمييز بين التشغيلات.

8. Data Quality وClassification

يصنف النظام كل سجل إلى إحدى الحالات التالية:

الحالة
المعنى
المكان النهائي
Valid
السجل صالح ولم يحتج إلى تعديل
orders_validated
Corrected
تم إصلاح السجل بقاعدة واضحة
orders_validated
Quarantine
لا يمكن تصحيح السجل بأمان
orders_quarantine




تتضمن قواعد التنظيف تحويل Arabic وPersian Digits، وتوحيد Currency، ومعالجة Thousand Separators، وتحويل الأسعار المكتوبة بالكلمات، وتطبيع Phone Number، وتصحيح Repeated Email Symbols، وتوحيد Date Format، وتنظيف Text Fields، وإعادة حساب Order Total عند صلاحية مكوناته.

لا يتم التصحيح عندما تكون القيمة غير قابلة للاستنتاج بأمان. في هذه الحالة ينتقل السجل إلى Quarantine مع سبب واضح.

9. Audit Trail

يحتوي كل Corrected Record على quality_status بقيمة corrected، وعلى corrections توضح الحقول التي تغيرت. يتضمن كل Correction عادة Field وOriginal Value وCorrected Value وRule Code.

بهذا لا يتم حفظ القيمة النهائية فقط، بل يمكن معرفة ما الذي تغير ولماذا تم تغييره.

10. Quarantine

يحتوي كل Quarantine Document على Run ID وSource File وSource Row Number وError Codes وError Details وRaw Record.

من أمثلة Error Codes المستخدمة MISSING_ORDER_ID وMISSING_CUSTOMER_ID وINVALID_IMPOSSIBLE_DATE وCORRUPTED_ITEMS_JSON وEMPTY_ITEMS وUNKNOWN_PRICE وAMBIGUOUS_NEGATIVE_VALUE وDUPLICATE_ORDER_ID وMULTIPLE_CONFLICTING_ERRORS وEMAIL_UNFIXABLE.

لا يتم حذف السجل غير القابل للتصحيح، بل يتم الاحتفاظ به للمراجعة داخل orders_quarantine.

11. MongoDB Design

يستخدم المشروع ثلاث Collections:

orders_raw لحفظ كل السجلات الخام مع معلومات المصدر والتشغيل.

orders_validated لحفظ السجلات Valid وCorrected القابلة للاستخدام.

orders_quarantine لحفظ السجلات التي لا يمكن تصحيحها بأمان.

يحتوي orders_validated على Schema Validation للحقول الأساسية، وعلى Unique Index باسم uniq_order_id على order_id. يستخدم order_id كـStable Business Key في عملية Upsert.

إذا لم يكن order_id موجودًا في orders_validated، يتم إدخال سجل جديد. وإذا كان موجودًا واختلف record_hash، يتم تحديث السجل الموجود. وإذا كان record_hash مطابقًا، يحسب النظام السجل Unchanged ولا ينشئ نسخة جديدة.

12. Idempotency وUpsert

تم اختبار تشغيل نفس الملف مرتين في Python Batch وPySpark. في التشغيل الثاني بقي عدد documents في orders_validated ثابتًا، ولم تظهر Duplicate Business Records.

تم أيضًا تعديل سجل موجود في قاعدة اختبار، ثم تشغيل نفس الملف مرة أخرى. أظهر اختبار PySpark count_updated بقيمة 1، مع count_inserted بقيمة 0، وبقاء عدد orders_validated ثابتًا. هذا يثبت أن Update يتم على السجل الموجود بدل إنشاء سجل جديد.

تختلف طبيعة Collections في هذا الجانب؛ orders_validated تمثل الحالة النهائية الفريدة، بينما orders_raw وorders_quarantine يمكن أن تحتفظا بسجل لكل Run لأغراض التتبع.

13. Metrics

يحفظ البرنامج نتائج كل Run في reports/results.json. تشمل Metrics Run ID واسم الملف وحجمه وProcessing Engine وRows Read وRaw Loaded وValid Count وCorrected Count وQuarantine Count وElapsed Seconds وThroughput وBatch Size أو Partitions وError Case Counts وInserted Count وUpdated Count وUnchanged Count وConsistency Check.

تستخدم Consistency Check العلاقة التالية:

Plain Text


raw_loaded = valid_count + corrected_count + quarantine_count



14. Resource Management

يتم إغلاق SparkSession واتصال MongoDB داخل finally في نقطة التشغيل. هذا يضمن إغلاق الموارد حتى عند حدوث خطأ أثناء المعالجة.

15. النتائج الرئيسية

تمت معالجة ملف يحتوي على 30,000,000 سجل باستخدام PySpark بنجاح. كانت النتائج 22,343,466 Valid، و5,697,571 Corrected، و1,958,963 Quarantine. مجموع الحالات الثلاث يساوي 30,000,000 سجل.

كما تم اختبار Classification Parity على عينة من 485 سجلًا، وكانت النتائج متطابقة بين Python Batch وPySpark.

