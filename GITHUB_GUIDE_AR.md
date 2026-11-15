# دليل رفع شغلك على GitHub (خطوة بخطوة)

## أين يتحط كل ملف
المشروع كله repository واحد. جزؤك:

```
lms-bigdata/
  contracts/                  <- sample_raw.jsonl, sample_clean.jsonl, README.md   (مشترك)
  1_producer/                 <- كل ملفات الكود بتاعك (شغلك إنت)
  .gitignore                  <- موجود، لو اتعمل واحد في الـ repo ادمجهم
  (infra/ و 2_nifi/ ... بتوع زمايلك، ما تلمسهمش)
```
قاعدة الفريق: **كل واحد بيعدّل في مجلده فقط**. ملفات `contracts/` تغييرها بيحتاج موافقة الستة.

## الحالة أ) العضو 6 (أو حد تاني) عمل الـ repo
1. **تثبيت Git:** نزّله من git-scm.com. وفي الـ terminal:
   ```bash
   git config --global user.name "اسمك"
   git config --global user.email "your@email.com"
   ```
2. اطلب من صاحب الـ repo يضيفك **Collaborator** (Settings > Collaborators)، وإنت تقبل الدعوة من الإيميل.
3. **استنسخ الـ repo:**
   ```bash
   git clone https://github.com/<اسم-المستخدم-أو-التيم>/lms-bigdata.git
   cd lms-bigdata
   ```
   لو طلب باسورد: GitHub مبقاش يقبل الباسورد العادي. استخدم **Personal Access Token** (Settings > Developer settings > Personal access tokens) مكان الباسورد، أو الأسهل: ثبّت GitHub CLI ونفّذ `gh auth login`.
4. **اعمل branch خاص بيك:**
   ```bash
   git checkout -b m1/producer
   ```
5. **انسخ شغلك:** فك الـ zip وانسخ مجلد `1_producer/` ومجلد `contracts/` جوّه الـ repo (لو `contracts/` موجود، انسخ الملفات اللي جواه بس).
   لو `.gitignore` موجود في الـ repo ما تكتبش فوقه، ضيف عليه السطور الناقصة.
6. **تأكد إن الشغل سليم قبل الرفع:**
   ```bash
   cd 1_producer && pip install -r requirements.txt && pytest -q && cd ..
   ```
7. **ارفعه:**
   ```bash
   git status                         # شوف اللي اتغيّر
   git add 1_producer contracts
   git commit -m "M1: LMS event simulator, Kafka producer, sample files"
   git push -u origin m1/producer
   ```
8. **افتح Pull Request:** GitHub هيظهرلك زرار "Compare & pull request" بعد الـ push. اكتب وصف قصير واطلب مراجعة من زميل (دورة المراجعة: عضو 6 هو اللي بيراجع عضو 1). بعد الموافقة اعمل **Merge** على `main`.
9. **قول للفريق** إن ملفات العينة نزلت في `contracts/` عشان كل واحد يعمل `git pull`.

## الحالة ب) محدش عمل repo لسه
1. على github.com اضغط **New repository**، الاسم `lms-bigdata`، اختار **Private**، وتجاهل خيارات README.
2. على جهازك:
   ```bash
   cd lms-bigdata                      # المجلد اللي فكّيته من الـ zip
   git init
   git branch -M main
   git add .
   git commit -m "M1: initial commit - simulator, producer, samples"
   git remote add origin https://github.com/<اسمك>/lms-bigdata.git
   git push -u origin main
   ```
3. أضف زمايلك: Settings > Collaborators > Add people (وعضو 6 يكمّل الباقي).

## الشغل اليومي بعد كده
```bash
git pull                                # هات آخر تعديلات الفريق (قبل ما تبدأ)
# ... عدّل في 1_producer فقط ...
git add 1_producer
git commit -m "M1: describe what you changed"
git push
```

## مشاكل شائعة
| المشكلة | الحل |
|---|---|
| `Authentication failed` | استخدم Personal Access Token بدل الباسورد، أو `gh auth login` |
| `rejected ... fetch first` | `git pull --rebase` وبعدين `git push` |
| `remote: Permission denied` | لسه ما اتضافتش Collaborator، أو قبلت الدعوة؟ |
| رفعت ملف بالغلط (باسورد مثلًا) | قول للفريق فورًا وغيّر الباسورد. مسح الملف من الـ commit الجديد مش كفاية لأنه بيفضل في التاريخ |
| ملفات كتيرة غريبة ظهرت في `git status` | تأكد إن `.gitignore` موجود (يستبعد `.venv` و`__pycache__`) |

## تذكير
* ما ترفعش باسوردات حقيقية ولا مجلد `.venv`.
* أول تسليم عليك: ملفات العينة. ارفعها بدري وقول للفريق.
