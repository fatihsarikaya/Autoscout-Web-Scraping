##############################################################################################################################
# 1. BÜTÜN MARKA-MODEL LİNKLERİNİ TOPLAMA  (Part 1)
 
#def get_all_make_model(mobile_de_eng_base_link="https://www.autoscout24.com/?genlnk=navi&genlnkorigin=com-all-all-home", save_filename="make_and_model_links.csv"):

from optparse import Values
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from datetime import datetime
import time
from bs4 import BeautifulSoup
import pymysql
import mysql.connector
import configparser
import pandas as pd
import numpy as np
import re
import itertools
from random import randrange
from tqdm import tqdm #progress bar
from selenium.webdriver.firefox.options import Options

#fireFoxOptions = Options()
#fireFoxOptions.binary_location = r'C:\Program Files\Firefox Developer Edition\firefox.exe'   # PC"ye Firefox Developer yüklenmelidir.
#fireFoxOptions.add_argument("--headless") 
##fireFoxOptions.add_argument("--window-size=1920,1080")
##fireFoxOptions.add_argument('--start-maximized')
#fireFoxOptions.add_argument('--disable-gpu')
#fireFoxOptions.add_argument('--no-sandbox')

#driver = webdriver.Firefox(options=fireFoxOptions)


options = webdriver.ChromeOptions()
prefs = {"profile.managed_default_content_settings.images": 2}
options.add_experimental_option("prefs", prefs)
service = Service(executable_path = r'C:\Users\Fatih\Desktop\mobile-de\chromedriver.exe')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

starting_link_to_scrape = "https://www.autoscout24.com/?genlnk=navi&genlnkorigin=com-all-all-home"
driver.get(starting_link_to_scrape)
time.sleep(0)
base_source = driver.page_source
base_soup = BeautifulSoup(base_source, 'html.parser')

make_list = base_soup.findAll('div', {'class': 'hf-searchmask-form__filter__make'})[0]
one_make = make_list.findAll('option')   # Marka listesini tıklamaya gerek kalmadan tüm markalar çekilir !!!

#print(one_make)

car_make = []   ### Audi, BMW, Ford....
id1 = []        ### 9, 13, 29 (id1 = marka id)

for i in range(len(one_make)):   

    car_make.append(one_make[i].text.strip())
    car_make=list(map(lambda x: x.replace(" ", "-"),car_make))   # Marka aralarındaki boşlukları tire olarak değiştirilir !!!

    try:
        id1.append(one_make[i]['value'])
    except:
        id1.append('')

car_base_make_data = pd.DataFrame({   # Marka ve id'leri Dataframe'e kaydedilir !!!
    'car_make': car_make, 
    'id1': id1
    })

car_make_filter_out = ['Make', 'Others', '']   # Filtreleme işlemi yapılır !!!
car_base_make_data = car_base_make_data[~car_base_make_data.car_make.isin(car_make_filter_out)]   # D.F'den filtreledigimiz ifadelerin listede olup olmadığını kontrol eder !!!
car_base_make_data = car_base_make_data.drop_duplicates()   # Aynı ifadeleri çıkarır !!!
car_base_make_data = car_base_make_data.reset_index(drop=True)   # Make, 'Others' ve '' ifadeleri eğer listede yoksa yani 'true' ise D.F'den düşürülmüş olur !!!


print(car_base_make_data)


car_base_model_data = pd.DataFrame()   # car_base_model_data adında bir d.f. olusturduk !!!

for one_make in tqdm(car_base_make_data['car_make'], "Progress: "): # tqdm'i döngünün ilerlemesini görmek için kullanıyoruz !!!

    #make_string = "//select[@id='make']".format(one_make)
    #driver.find_element(By.XPATH,make_string).click()
    #time.sleep(3)
    
    len_car_base_make_data = len(car_base_make_data)
    print(" len_car_base_make_data:",len_car_base_make_data)
    
    l=0
    
    
    x = 233   # x = 4  yazılırsa eğer  ; ilk 3 arabanın marka-model linkleri db'e kayıt olunur. ( Audi, BMW ve Ford )

            # 232 marka araba var !!! ( x = 233 yazarak 232 marka aracın yani tüm araçların marka-model linklerini çekebiliriz )
    
    while l < x:  
        l += 1;
    
    
    #for l in range(len_car_base_make_data):
        
        grbf = Select(driver.find_element(By.XPATH,"//select[@id='make']"))   # Sırayla markalara tıklatma işlemi yapılır !!!
        grbf.select_by_value(id1[l]) #   id1[1] = Audi' 
        
        try:
            print(" Sirayla markalar seciliyor, " + car_make[l] + " secildi")
            
        except:
            print(' Not clicked any make')
        
        
        time.sleep(1) # wait for the page to load   
        
        base_source = driver.page_source
        base_soup = BeautifulSoup(base_source, 'html.parser')

        model_list = base_soup.findAll('div', {'class': 'hf-searchmask-form__filter__model'})[0]
        models = model_list.findAll('option')   # Sırayla her tıklanan markanın modelleri çekilir !!!

        #try:
        #    print(models)
        #except:
        #    print("Not listed any model")        
    
        car_model = []
        id2 = []   # (id2 = model id)

        for i in range(len(models)):   # Ne kadar model sayısı varsa o uzunlukça modeller her marka içın sıralanır !!!
            
            car_model.append(models[i].text.strip())
            car_model=list(map(lambda x: x.replace(" ", "-"),car_model))     # Model aralarındaki boşlukları tire olarak değiştirilir !!!
            #car_model=list(itertools.filterfalse(lambda x: x.endswith('(all)'), car_model))
            #car_model=list(filter(lambda x: not x.endswith('(all)'), car_model))

            try:
                id2.append(models[i]['value'])
                #id2=list(filter(lambda x: not x.startswith('g'), id2))
            except:
                id2.append('')

        car_base_model_data_aux = pd.DataFrame({    # Model ve id'leri bir başka Dataframe'e kaydedilir !!!
            'car_model': car_model, 
            'id2': id2
            })
                                # Filtreleme işlemi tekrar yapılır !!!
        car_model_filter_outside = ['Model',' ','1-Series-(all)','2-Series-(all)','3-Series-(all)','4-Series-(all)','5-Series-(all)','6-Series-(all)','7-Series-(all)','8-Series-(all)','M-Series-(all)','X-Series-(all)','Z-Series-(all)','Ranger-(all)','Tourneo-(all)','Transit-(all)','A-Series-(all)','B-Series-(all)','C-Series-(all)','CE-(all)','CL-(all)','CLA-(all)','CLK-(all)','CLS-(all)','E-Series-(all)','EQ-Series-(all)','G-Series-(all)','GL-(all)','GL-(all)','GLB-(all)','GLC-(all)','GLE-(all)','GLK-(all)','GLS-(all)M-Series-(all)','R-Series-(all)','S-Series-(all)SL-(all)','SLC-(all)','SLK-(all)','V-Series-(all)','X-Series-(all)','Golf-(all)','ID.-Buzz-(all)','Passat-(all)','Polo-(all)','T3-Series-(all)','T4-Series-(all)','T5-Series-(all)','T6-Series-(all)','T7-Series-(all)','Tiguan-(all)','C3-(all)','C4-(all)','C5-(all)','ES-Series-(all)','GS-Series-(all)','GX-Series-(all)','IS-Series-(all)','LC-Series-(all)','LS-Series-(all)','LX-Series-(all)','NX-Series-(all)','RC-Series-(all)','RX-Series-(all)','SC-Series-(all)','UX-Series-(all)','Aston-Martin-(all)','Bentley-(all)','BMW-(all)','Ferrari-(all)','Lotus-(all)','Maserati-(all)','Mercedes-Benz-(all)','Porsche-(all)','Rolls-Royce-(all)','Cabrio-Series-(all)','Clubman-Series-(all)','Countryman-Series-(all)','Coupé Series (all)','Paceman-Series-(all)','Roadster-Series-(all)','718-(all)','911-Series-(all)','GLA-(all)','GLS-(all)','S-Series-(all)','SL-(all)','Coupé-Series-(all)']
        car_base_model_data_aux = car_base_model_data_aux[~car_base_model_data_aux.car_model.isin(car_model_filter_outside)]
        car_base_model_data_aux = car_base_model_data_aux.drop_duplicates()
        car_base_model_data_aux = car_base_model_data_aux.reset_index(drop=True)
        
        
        car_base_model_data_aux['car_make'] = car_make[l]    # 2. D.F'inde car_make ortak sütünunda markalar listelenir !!!
        
           # "car_base_model_data" d.f'ine , model ve id'lerın geldiği "car_base_model_data_aux" d.f.'i her loop sonrası üst üst eklenir !!!
        car_base_model_data = pd.concat([car_base_model_data, car_base_model_data_aux], ignore_index=True) 
        
        print(car_base_model_data)   # Her marka için gelen modeller sırayla ekranda gösterilir !!!
        
        #print(car_base_model_data[car_base_model_data['car_make'] == "Audi"])
        
        time.sleep(0)

        if l == x-1 :
            break
    print("End")
    break 
#print('Out of loop')



                # En son gelen 2 Dataframe birleştirilir !!!
car_data_base = pd.merge(car_base_make_data, car_base_model_data, left_on=['car_make'], right_on=['car_make'], how='right')
car_data_base = car_data_base[~car_data_base.id2.isin([""])]   # id2'lerin boş olmadığını kontrol eder. (true döner)  
car_data_base = car_data_base[car_data_base.id2.apply(lambda x: x.isnumeric())]   # id2'leri nümerik hale getirir !!!
car_data_base = car_data_base.drop_duplicates()

                # Link yapısı oluşturma !!!
car_data_base['link'] = "https://www.autoscout24.com/lst/" + car_data_base['car_make'] + "/" + car_data_base['car_model'] + "?sort=mileage&desc=1&cy=D&atype=C" # &desc=1& ise descending e göre ilan linklerini sıralar.
car_data_base = car_data_base.reset_index(drop=True)   # Eski dizinin bir sütun olarak eklenmesini önlemek için drop parametresini kullanıyoruz !!!  

                # car_data_base DataFrame'ini cvs formatında kaydetme !!!                                                    
#if len(save_filename) > 0:
#car_data_base.to_csv(r'C:\Users\*******\Desktop\make_and_model_links_country_germany_mileage_desc.csv', encoding='utf-8', index=False)
#return(car_data_base)


#   Datetime string
now = datetime.now() 
datetime_string = str(now.strftime("%Y%m%d_%H%M%S"))
#   Datetime'ı kolon olarak df'e eklemek için !!!
car_data_base['download_date_time'] = datetime_string


print("car_data_base:",car_data_base)



config = configparser.RawConfigParser()
config.read(filenames = 'my.properties')


scrap_db = pymysql.connect(host='localhost',user='root',password='1234',database='autoscout',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

cursor = scrap_db.cursor()



sql = """CREATE TABLE carlist_de_mileage_desc(
        
        id int(11),
        brand VARCHAR(32),
        brand_id VARCHAR(16),
        model VARCHAR(32),
        model_id VARCHAR(16),
        link VARCHAR(255),
        created_at VARCHAR(32),
        updated_at datetime,
        status tinyint(3)
        )"""
        
cursor.execute(sql)

for row_count in range(0, 1): 
        chunk = car_data_base.iloc[row_count:row_count + 1,:].values.tolist()
        
        #print("-------------------------------------------------------")
        #print(chunk[0])
        #print("-------------------------------------------------------")
        
        id = 0
        brand = ""
        brand_id = ""
        model = ""
        model_id = ""
        link = ""
        created_at = ""
        updated_at = ""
        status = ""
        
        
        #control = "true"
        
        
        len_for_links = len(car_data_base.link)
        
        for l in range(len_for_links):
            
            id +=1
                        
            if "car_make" in car_data_base: 
                try:
                    brand = car_data_base.car_make[l]
                except:
                    brand = ""
            
            if "id1" in car_data_base:
                try:
                    brand_id = car_data_base.id1[l]
                except:
                    brand_id = ""
                    
            if "car_model" in car_data_base:
                try:
                    model = car_data_base.car_model[l]
                except:
                    model = ""
            
            if "id2" in car_data_base:
                try:
                    model_id = car_data_base.id2[l]
                except:
                    model_id = ""
                    
            if "link" in car_data_base:
                try:
                    link = car_data_base.link[l]
                except:
                    link = ""
                    
            
            if "download_date_time" in car_data_base:
                try:
                    created_at = car_data_base.download_date_time[l]
                except:
                    created_at = ""
            
        
            mySql_insert_query = "INSERT INTO carlist_de_mileage_desc (id,brand,brand_id,model,model_id,link,created_at,updated_at,status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val =                                                     (id,brand,brand_id,model,model_id,link,created_at,updated_at,status)

            #cursor = scrap_db.cursor()
            cursor.execute(mySql_insert_query, val) # cursor.executemany(mySql_insert_query, tuple_of_tuples)
            
            scrap_db.commit()
            print(cursor.rowcount, "Record inserted successfully into *carlist_de_mileage_desc* table")

            #Disconnect from server
            #scrap_db.close()
        

