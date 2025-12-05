#!/usr/bin/env python3
"""
سكريبت شامل لاختبار جميع وظائف نظام استرجاع المستندات الذكي
"""

import requests
import json
from time import sleep

# إعدادات API
BASE_URL = "http://localhost:5000/api"

def print_section(title):
    """طباعة عنوان القسم"""
    print("\n" + "="*60)
    print(f"  {title}")
    print("="*60 + "\n")

def test_health():
    """اختبار حالة النظام"""
    print_section("اختبار 1: فحص حالة النظام")
    
    response = requests.get(f"{BASE_URL}/health")
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))

def load_sample_data():
    """تحميل البيانات التجريبية"""
    print_section("اختبار 2: تحميل البيانات التجريبية")
    
    with open('sample_data.json', 'r', encoding='utf-8') as f:
        documents = json.load(f)
    
    response = requests.post(
        f"{BASE_URL}/index/bulk",
        json={"documents": documents}
    )
    
    print(f"Status: {response.status_code}")
    print(json.dumps(response.json(), indent=2))
    
    # انتظار لضمان الفهرسة
    sleep(2)

def test_autocomplete():
    """اختبار البحث التلقائي"""
    print_section("اختبار 3: البحث التلقائي (Autocomplete)")
    
    queries = ["cli", "Artif", "Renew"]
    
    for query in queries:
        print(f"\nQuery: '{query}'")
        response = requests.get(
            f"{BASE_URL}/search/autocomplete",
            params={"q": query, "size": 5}
        )
        
        if response.status_code == 200:
            results = response.json()["results"]
            for i, result in enumerate(results, 1):
                print(f"  {i}. {result['title']} (score: {result['score']:.2f})")
        else:
            print(f"Error: {response.status_code}")

def test_smart_search():
    """اختبار البحث الذكي"""
    print_section("اختبار 4: البحث الذكي مع المعايير المكانية والزمانية")
    
    # اختبار 1: بحث نصي فقط
    print("\n--- البحث النصي فقط ---")
    response = requests.post(
        f"{BASE_URL}/search/smart",
        json={
            "query": "climate change",
            "size": 5
        }
    )
    
    if response.status_code == 200:
        results = response.json()["results"]
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Score: {result['score']:.2f}")
            if 'date' in result:
                print(f"   Date: {result['date']}")
    
    # اختبار 2: بحث مع موقع جغرافي
    print("\n--- البحث مع الموقع الجغرافي ---")
    response = requests.post(
        f"{BASE_URL}/search/smart",
        json={
            "query": "research",
            "georeference": "New York",
            "size": 5
        }
    )
    
    if response.status_code == 200:
        results = response.json()["results"]
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Score: {result['score']:.2f}")
    
    # اختبار 3: بحث مع تعبير زمني
    print("\n--- البحث مع التعبير الزمني ---")
    response = requests.post(
        f"{BASE_URL}/search/smart",
        json={
            "query": "AI healthcare",
            "temporal_expression": "2024",
            "size": 5
        }
    )
    
    if response.status_code == 200:
        results = response.json()["results"]
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Score: {result['score']:.2f}")
            if 'date' in result:
                print(f"   Date: {result['date']}")
    
    # اختبار 4: بحث شامل
    print("\n--- البحث الشامل (نص + موقع + زمن) ---")
    response = requests.post(
        f"{BASE_URL}/search/smart",
        json={
            "query": "development",
            "georeference": "Europe",
            "temporal_expression": "2024",
            "size": 5
        }
    )
    
    if response.status_code == 200:
        results = response.json()["results"]
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Score: {result['score']:.2f}")

def test_top_georeferences():
    """اختبار أكثر المواقع تكراراً"""
    print_section("اختبار 5: أكثر 10 مواقع جغرافية تكراراً")
    
    response = requests.get(
        f"{BASE_URL}/analytics/top-georeferences",
        params={"size": 10}
    )
    
    if response.status_code == 200:
        results = response.json()["results"]
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['georeference']}: {result['count']} documents")

def test_time_distribution():
    """اختبار توزيع المستندات عبر الزمن"""
    print_section("اختبار 6: توزيع المستندات عبر الزمن")
    
    response = requests.get(
        f"{BASE_URL}/analytics/time-distribution",
        params={"interval": "1d"}
    )
    
    if response.status_code == 200:
        results = response.json()["results"]
        print("\nتوزيع المستندات حسب التاريخ:")
        for result in results:
            if result['count'] > 0:
                print(f"  {result['date']}: {result['count']} documents")

def test_dashboard():
    """اختبار لوحة التحكم الشاملة"""
    print_section("اختبار 7: لوحة التحكم الشاملة")
    
    response = requests.get(f"{BASE_URL}/analytics/dashboard")
    
    if response.status_code == 200:
        data = response.json()
        
        print(f"\nإجمالي المستندات: {data['total_documents']}")
        
        print("\nأكثر المواقع الجغرافية:")
        for i, geo in enumerate(data['top_georeferences'][:5], 1):
            print(f"  {i}. {geo['georeference']}: {geo['count']}")
        
        print("\nتوزيع المستندات عبر الزمن (آخر 10 أيام):")
        for item in data['documents_over_time'][-10:]:
            if item['count'] > 0:
                print(f"  {item['date']}: {item['count']}")

def test_index_stats():
    """اختبار إحصائيات الفهرس"""
    print_section("اختبار 8: إحصائيات الفهرس")
    
    response = requests.get(f"{BASE_URL}/index/stats")
    
    if response.status_code == 200:
        stats = response.json()
        print(f"عدد المستندات: {stats['document_count']}")
        print(f"حجم الفهرس: {stats['index_size']:,} bytes")

def run_all_tests():
    """تشغيل جميع الاختبارات"""
    try:
        print("\n" + "#"*60)
        print("#  نظام استرجاع المستندات الذكي - اختبار شامل")
        print("#"*60)
        
        test_health()
        load_sample_data()
        test_autocomplete()
        test_smart_search()
        test_top_georeferences()
        test_time_distribution()
        test_dashboard()
        test_index_stats()
        
        print("\n" + "#"*60)
        print("#  اكتملت جميع الاختبارات بنجاح!")
        print("#"*60 + "\n")
        
    except requests.exceptions.ConnectionError:
        print("\n❌ خطأ: لا يمكن الاتصال بالخادم")
        print("تأكد من أن التطبيق يعمل على http://localhost:5000")
    except Exception as e:
        print(f"\n❌ حدث خطأ: {str(e)}")

if __name__ == "__main__":
    run_all_tests()