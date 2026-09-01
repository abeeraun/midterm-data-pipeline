نتائج المشروع

ملخص التنفيذ

تم تنفيذ خط البيانات باستخدام مسارين للمعالجة. يستخدم المشروع Python Batch للملفات الصغيرة، ويستخدم PySpark للملفات الكبيرة. يتم حفظ البيانات في MongoDB ضمن ثلاث Collections رئيسية: orders_raw وorders_validated وorders_quarantine.

تم استخدام order_id كـBusiness Key، مع Unique Index في orders_validated، كما تم تفعيل Schema Validation للحقول الأساسية.

نتيجة تشغيل الملف الكبير

تمت معالجة الملف data/orders_huge_mixed_quality.csv باستخدام PySpark.

المقياس
النتيجة
حجم الملف
12,650.32 MB
عدد السجلات المقروءة
30,000,000
عدد السجلات في orders_raw
30,000,000
Valid
22,343,466
Corrected
5,697,571
Quarantine
1,958,963
Input partitions
99
Processing time
19,913.98 ثانية
Throughput
1,506.48 سجل في الثانية
Inserted into orders_validated
28,041,037




نجح Consistency Check:

Plain Text


30,000,000 = 22,343,466 + 5,697,571 + 1,958,963



كما أن:

Plain Text


28,041,037 = 22,343,466 + 5,697,571



انتهى التشغيل بنجاح، وتم إغلاق SparkSession واتصال MongoDB.

مقارنة Python Batch وPySpark

تم تشغيل المحركين على نفس العينة التي تحتوي على 485 سجلًا.

Processing engine
Valid
Corrected
Quarantine
Total
Python Batch
382
84
19
485
PySpark
382
84
19
485




أصبح عدد السجلات التي اختلف تصنيفها بين المحركين يساوي صفرًا، بعد توحيد قواعد معالجة التاريخ، والقيم المالية، والأرقام العربية، وitems_json، والقيم السالبة، وDuplicate order_id.

اختبارات الحجم

نجح اختبار PySpark على 100,000 سجل، كما نجح اختبار PySpark على 1,000,000 سجل. تم استخدام هذه الاختبارات للتحقق من قدرة المسار الموزع قبل تشغيل الملف الكامل.

نتائج اختبار المليون كانت 744,717 Valid، و189,948 Corrected، و65,335 Quarantine، ومجموعها 1,000,000 سجل.

Idempotency وUpsert

تم اختبار إعادة تشغيل نفس الملف في Python Batch وPySpark. في إعادة التشغيل لم يزد عدد المستندات في orders_validated، ولم تظهر Business Records مكررة.

في اختبار Python Batch الثاني ظهرت النتائج التالية:

Plain Text


count_inserted = 0
count_updated = 0
count_unchanged = 466



وفي اختبار PySpark الثاني ظهرت النتائج التالية:

Plain Text


count_inserted = 0
count_updated = 0
count_unchanged = 466



بقي عدد المستندات في orders_validated يساوي 466. أما زيادة orders_raw وorders_quarantine عند إعادة التشغيل فهي متوقعة لأنهما تحتفظان بسجل كل Run.

تم أيضًا اختبار Update على سجل موجود في Python Batch وPySpark. في اختبار PySpark ظهرت النتائج التالية:

Plain Text


count_inserted = 0
count_updated = 1
count_unchanged = 465



بقي عدد المستندات في orders_validated يساوي 466، مما يثبت أن السجل تم تحديثه باستخدام Upsert دون إنشاء سجل جديد.

الاختبارات البرمجية

نجحت اختبارات المشروع:

Plain Text


25 passed



كما نجحت اختبارات Classification Parity وIdempotency وDuplicate Prevention وUpdate في مساري Python Batch وPySpark.

Collections وقيود MongoDB

تحتوي orders_raw على السجلات الأصلية مع بيانات Run والمصدر.

تحتوي orders_validated على السجلات Valid وCorrected مع Quality Status وCorrection Details.

تحتوي orders_quarantine على السجلات غير المقبولة مع Error Codes وError Details وRaw Record.

يمنع Unique Index على order_id تكرار Business Records في orders_validated، بينما يسمح تصميم Raw وQuarantine بالاحتفاظ بتاريخ كل Run.

الخلاصة

نجح المشروع في معالجة ملف يحتوي على 30,000,000 سجل باستخدام PySpark. تم التحقق من Consistency Check، وتطابق قواعد Python وSpark، ونجاح Idempotency وUpsert، وعدم وجود Business Records مكررة في orders_validated.

