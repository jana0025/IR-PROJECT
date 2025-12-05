FROM python:3.10-slim

WORKDIR /app

# تثبيت المكتبات المطلوبة للنظام
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    curl \
    && rm -rf /var/lib/apt/lists/*

# نسخ ملف المتطلبات
COPY requirements.txt .

# تحديث pip
RUN pip install --upgrade pip

# تثبيت المكتبات Python
RUN pip install --no-cache-dir -r requirements.txt

# تنزيل نموذج spaCy - الطريقة الصحيحة
RUN pip install https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.7.1/en_core_web_sm-3.7.1-py3-none-any.whl

# نسخ الملفات
COPY . .

# فتح المنفذ
EXPOSE 5000

# تشغيل التطبيق
CMD ["python", "app.py"]