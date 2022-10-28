from tkinter import E
from xml.etree.ElementTree import QName
import bs4
import urllib.request
import pandas as pd
import pymysql
import mysql.connector
import configparser
import re
import numpy as np
import time
import concurrent.futures
#import erequests
import lxml
from multiprocessing import Pool
#from multiprocessing import Process, Lock
from multiprocessing import Process
from datetime import datetime
from tqdm import tqdm #progress bar
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime
from sqlalchemy import create_engine
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.firefox.options import Options


mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="1234",
  database="autoscout"
)
mycursor = mydb.cursor()

sql = "SELECT ad_link FROM adlinks_de_mileage_asc"

mycursor.execute(sql)

#Fetching 1st row from the table
#result = mycursor.fetchmany(size =2)

myresult = mycursor.fetchall()


all_links = myresult[0:]


len_all_links = len(all_links)

#print("firstlink:",myresult[0])

#print("len_all_links:",len_all_links)

#print("all_links:",all_links)


dataframe = pd.DataFrame(all_links, columns=['links'])

#print("dataframe:",dataframe)


#print("ilklink:",dataframe.links[0])


x = 0
y = 324460


def fonksyion(i):  # def fonksyion(x,y):
    
    
    #x = 0
    #y = 5

    #number = np.arange(x,y) #324460
        
    #for i in tqdm(number):
        ad_link = dataframe.links[i]   #  ad_link = dataframe["links"][i]
        #print(ad_link)

        fireFoxOptions = Options()
        fireFoxOptions.binary_location = r'C:\Program Files\Firefox Developer Edition\firefox.exe' # PC"ye Firefox Developer yüklenmelidir. 
        fireFoxOptions.add_argument("--headless") 
        #fireFoxOptions.add_argument("--window-size=1920,1080")
        #fireFoxOptions.add_argument('--start-maximized')
        fireFoxOptions.add_argument('--disable-gpu')
        fireFoxOptions.add_argument('--no-sandbox')
        
        driver = webdriver.Firefox(options=fireFoxOptions)
       
        
        sleep_time = 0
            
#        options = Options()
#        options = webdriver.ChromeOptions()
        
        #options.headless = True
        #options.add_argument("--window-size=1920,1080")
        #options.add_argument("--headless")
        #options.add_argument("--disable-gpu")
        #options.add_argument("--no-sandbox")
        
        prefs = {"profile.managed_default_content_settings.images": 2} # this is to not load images
#        options.add_experimental_option("prefs", prefs)

        #start a driver

#        service = Service(executable_path='C:/Users/Fatih/Desktop/autoscout24/chromedriver.exe')
#        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

        #Go to webpage and scrape data


        # Test for Audi 100
        #html = 'https://www.autoscout24.com/offers/audi-100-avant-2-6-e-quattro-tuev07-2024-2-hand-ahk-klimaau-gasoline-red-5c71ae6d-2317-49de-b45b-cd3eef70cb8a?sort=standard&desc=D&lastSeenGuidPresent=true&cldtidx=1&position=1&search_id=19ctily0e8&source_otp=t40&source=listpage_search-results'
        #driver.get(html)


        driver.get(ad_link)
        time.sleep(sleep_time)
        ad_source = driver.page_source
        ad_soup = BeautifulSoup(ad_source, 'lxml') #html.parser



        mainresults = ad_soup.find_all('div',{'class' : 'StageArea_informationContainer__VaFP8'})
            
            
        try:
            brand = mainresults[0].find("span", { "class" : re.compile('StageTitle_boldClassifiedInfo__L7JmO')}).get_text()
        except:
            brand = ' '
            

        try:
            model = mainresults[0].find("span", { "class" : re.compile('StageTitle_model__pG_6i StageTitle_boldClassifiedInfo__L7JmO')}).get_text()
        except:
            model = ' '
            
        try:
            model_version = mainresults[0].find("div", { "class" : re.compile('StageTitle_modelVersion__Rmzgd')}).get_text()
        except:
            model_version = ' '
            
        try:
            location = mainresults[0].find("a", { "class" : re.compile('scr-link LocationWithPin_locationItem__pHhCa')}).get_text()
        except:
            location = ' '

        try:
            price = mainresults[0].find("span", { "class" : re.compile('StandardPrice_price__X_zzU')}).get_text()
        except:
            price = ' '




        try: 
            dealer = ad_soup.find("div", { "class" : re.compile('CommonComponents_nameContainer__3Z_zp')}).get_text()
        except:
            dealer = ' '
            
        try: 
            contact_name = ad_soup.find("span", { "class" : re.compile('Contact_contactName__MFXhS')}).get_text() #1
        except:
            contact_name = ' '
            
        try:
            tel_number = mainresults[0].find("a", { "class" : re.compile('scr-button scr-button--primary CTAs_linkColorOverride__jcjq2 CTAs_buttonPadding__6WE_j')}).get_text()
        except:
            tel_number = ' '  
            
            

        cars_data = pd.DataFrame({
                'brand': brand, 
                'model': model,
                'model_version': model_version,
                'location': location,
                'price': price,
                'dealer' : dealer,
                'contact_name' : contact_name,
                'tel_number':tel_number,
                }, 
                index=[0])
            
        #print("cars_data: ", cars_data)



        ######################################################################################################

        try:
            table_pre = ad_soup.find("div", { "class" : "VehicleOverview_containerMoreThanFourItems__QgCWJ"}) #1 (6 in one)
            all_div = table_pre.findAll("div", { "class" : re.compile('VehicleOverview_itemContainer__Ol37r')}) #6 (2 in one)
            all_title = table_pre.findAll("div", { "class" : re.compile('VehicleOverview_itemTitle__W0qyv')}) #6 
            all_results = table_pre.findAll("div", { "class" : re.compile('VehicleOverview_itemText__V1yKT')}) #6
            
        except:
            table_pre = []
            all_div = []
            

        description_list = []
        value_list = []


        try:
            div_length = len(all_div)
        except:
            div_length = 6 # 2

        i = 1
        for i in range(div_length):
            try:
                description_list.append(all_title[i].text)
                description_list=list(map(lambda x: x.replace(" ", "_"),description_list))
                value_list.append(all_results[i].text)
                i += 1  #2
            except:
                description_list.append('') # no_description
                value_list.append('') # no_value

        #print("description_list:", description_list)
        #print("value_list:", value_list)

        ############################################################################################################################

        try:
            
            #table_pr = ad_soup.find("div", { "class" : "DetailsSection_childrenSection__NQLD7"}) #6
            #alldivs = table_pr.findAll("dl", { "class" : re.compile('DataGrid_defaultDlStyle__969Qm')}) #6
            all_keys = ad_soup.find_all('dt',{'class' : re.compile('DataGrid_defaultDtStyle__yzRR_')}) #sayi degisiyor
            all_values = ad_soup.find_all("dd", { "class" : re.compile('DataGrid_defaultDdStyle__29SKf DataGrid_fontBold__r__dO')})#[0].get_text() #
            
        except:
            
            all_keys = []
            all_values = []


        all_key = []
        all_value = []

            

        try:
            div_lengths = len(all_keys)
        except:
            div_lengths = 33 # = 2

        j = 1
        #filter_words = ("Comfort & Convenience","Entertainment & Media","Safety & Security","Extras")
        for j in range(div_lengths):
            try:
                #all_key=list(filter(lambda x: (x != filter_words),all_key))
                
                all_key=list(map(lambda x: x.replace("CO₂_efficiency", "CO2_emissions"),all_key))
                all_key=list(map(lambda x: x.replace("CO₂_emissions", "CO2_efficiency"),all_key))
                
                #all_key=list(map(lambda x: x.replace("Colour", "colour_and_upholstery"),all_key))
                #all_key=list(map(lambda x: x.replace("Manufacturer_colour", "colour_and_upholstery"),all_key))
                #all_key=list(map(lambda x: x.replace("Paint", "colour_and_upholstery"),all_key))
                #all_key=list(map(lambda x: x.replace("Upholstery_colour", "colour_and_upholstery"),all_key))
                #all_key=list(map(lambda x: x.replace("Upholstery", "colour_and_upholstery"),all_key))
                
                all_key=list(map(lambda x: x.replace("Comfort_&_Convenience", "colour_and_upholstery"),all_key))
                all_key=list(map(lambda x: x.replace("Entertainment_&_Media", "colour_and_upholstery"),all_key))
                all_key=list(map(lambda x: x.replace("Safety_&_Security", "colour_and_upholstery"),all_key))
                all_key=list(map(lambda x: x.replace("Extras", "colour_and_upholstery"),all_key))
                
                
                all_key.append(all_keys[j].get_text())
                
                all_key=list(map(lambda x: x.replace(" ", "_"),all_key))
                all_key=list(map(lambda x: x.replace("-", "_"),all_key))

                
                all_value.append(all_values[j].text)
                
                j += 1  #2
            except:
                all_key.append('colour_and_upholstery') # no_key      # colour_and_upholstery
                all_value.append('') # no_value
                
        #print("all_key:", all_key)
        #print("all_value:", all_value)
                

        ##################################################################################################################
        soup  = BeautifulSoup(ad_source, 'lxml') #driver.page_source, '' #html.parser
        try: 
            litag = soup.find('dl',{'class' : 'DataGrid_defaultDlStyle__969Qm DataGrid_asColumnUntilLg__ontpW'})
            li = litag.findAll('li')
            equipment_keys = ad_soup.find_all('dt',{'class' : 'DataGrid_defaultDtStyle__yzRR_ DataGrid_fontBold__r__dO'})
        except:
            equipment_keys = []
            

        equipment_key = []


        try:
            equipment_key_length = len(li)  # len(equipment_keys) = aslinda max 4 olmali ama bu yontemle kac adet li varsa hepsine baslik olarak "all_equipment" getir ve degerleri virgulle tek bir hücrede yan yana yazdırıyoruz
        except:
            equipment_key_length = 1

        k = 1
        for k in range(equipment_key_length):
            try:    
                equipment_key.append(equipment_keys[k].get_text())
                #equipment_key=list(map(lambda x: x.replace(" & ", "_"),equipment_key))
                equipment_key=list(map(lambda x: x.replace("Comfort & Convenience", "all_equipment"),equipment_key))
                equipment_key=list(map(lambda x: x.replace("Entertainment & Media", "all_equipment"),equipment_key))
                equipment_key=list(map(lambda x: x.replace("Safety & Security", "all_equipment"),equipment_key))
                equipment_key=list(map(lambda x: x.replace("Extras", "all_equipment"),equipment_key))
                
                k += 1  
            except:
                equipment_key.append('all_equipment') #no_equipment_key
                
        #print("equipment_key:", equipment_key)
                
        ##################################################################################################################
        #soup  = BeautifulSoup(driver.page_source, 'lxml')
        try: 
            
            litag = ad_soup.find('dl',{'class' : 'DataGrid_defaultDlStyle__969Qm DataGrid_asColumnUntilLg__ontpW'})
            li = litag.findAll('li')
            
            
        except:
            #equipment_values= []
            litag = []
            #li= []

        equipment_value = []
            

        try:
            dd_ul_li_length = len(li)
        except:
            dd_ul_li_length = 1
            
        #equipment_lis = ("Electrical side mirrors")
        l = 1
        for l in range(dd_ul_li_length):
            try:    
                equipment_value.append(li[l].get_text())
                #equipment_value=list(filter(lambda x: (x == equipment_lis),equipment_value))
                
                l += 1  
            except:
                equipment_value.append('') #no_equipment_value



        #print("equipment_value:", equipment_value)

        ##################################################################################################################

        df3 = pd.DataFrame(list(zip(equipment_key, equipment_value)), columns = ['all_key', 'all_value'])      


        df2 = pd.DataFrame(list(zip(all_key, all_value)), columns = ['all_key', 'all_value'])



        #create a dataframe
        df1 = pd.DataFrame(list(zip(description_list, value_list)), columns = ['description_list', 'value_list'])



        # Sütun adları olarak -description_list- den gelen verileri transpose et
        #df = df.T
        df1 = df1.set_index('description_list').T.reset_index(drop=True)
        df1 = df1.rename_axis(None, axis=1)
        #df1['link'] = ad_link
        #######
        df1.insert(0,"brand",brand)
        df1.insert(1,"model",model) 
        df1.insert(2,"model_version",model_version)
        df1.insert(3,"location",location)
        df1.insert(4,"price",price)
        df1.insert(5,"dealer",dealer)
        df1.insert(6,"contact_name",contact_name)
        df1.insert(7,"tel_number",tel_number)


        ##################################################
        df2_3 = pd.concat([df2, df3])   #concat
        df2_3 = df2_3.set_index('all_key').T.reset_index(drop=True)
        df2_3 = df2_3.rename_axis(None, axis=1)



        df_last = pd.concat([df1, df2_3], axis=1)# join_axes=[df1.index])


        df_last = df_last.astype(str).groupby(df_last.columns, sort=False, axis=1).agg(lambda x: x.apply(','.join, 1)) #####BAK


        #datetime string

        now = datetime.now()
        datetime_string = str(now.strftime("%Y%m%d_%H%M%S"))

        try: 
            Vehicle_Description = ad_soup.find("div", { "class" : re.compile('SellerNotesSection_content__S5suY')}).get_text()
        except:
            Vehicle_Description = ' '
            
        ###############################################################  
        try:
            Picture_count = ad_soup.find_all("div", { "class" : re.compile('image-gallery-slide')})[0]
        except:
            Picture_count = ' '

        #images = ad_soup.findAll('img')
        #images = Picture_count.findAll('img')
        try: 
            Car_Picture_Link = Picture_count.find('img').attrs['src']
        except:
            Car_Picture_Link = ' '  #Picture_count
            

        df_last['vehicle_description'] = Vehicle_Description
        df_last['car_picture_link'] = Car_Picture_Link  
        df_last['ad_link'] = ad_link
        df_last['download_date_time'] = datetime_string

        #####################################################################################

        #try: 
        #    equipmentkeys = ad_soup.find_all('dt',{'class' : 'DataGrid_defaultDtStyle__yzRR_ DataGrid_fontBold__r__dO'})
        #except:
        #    equipmentkeys = []
            
        try: 
            equipmentkey = ad_soup.find_all('dt',{'class' : 'DataGrid_defaultDtStyle__yzRR_ DataGrid_fontBold__r__dO'})
        except:
            equipmentkey = []


        len_equipment_class=len(equipmentkey)

        equipmentkeyfirst = []
        quipmentkeysecond = []
        quipmentkeythird = []
        quipmentkeyfourth = []


        try:    
            equipmentkeyfirst.append(equipmentkey[0].get_text())
            quipmentkeysecond.append(equipmentkey[1].get_text())
            quipmentkeythird.append(equipmentkey[2].get_text())
            quipmentkeyfourth.append(equipmentkey[3].get_text())
            
        except:
            equipmentkeyfirst.append('') 
            quipmentkeysecond.append('') 
            quipmentkeythird.append('') 
            quipmentkeyfourth.append('')
                
                
        #print("equipmentkeyfirst:", equipmentkeyfirst)
        #print("quipmentkeysecond:", quipmentkeysecond)
        #print("quipmentkeythird:", quipmentkeythird)
        #print("quipmentkeyfourth:", quipmentkeyfourth)

        #print("equipmentkeylenght:",len_equipment_class)
                
        ##################################################################################################################
        try: 
            li_div =  ad_soup.find('dl',{'class' : 'DataGrid_defaultDlStyle__969Qm DataGrid_asColumnUntilLg__ontpW'})
            dd_count = li_div.find_all('dd',{'class' : 'DataGrid_defaultDdStyle__29SKf'})
            
        except:
            li_div = []

        #soup  = BeautifulSoup(driver.page_source, 'lxml')
        try: 
            
            li_div =  ad_soup.find('dl',{'class' : 'DataGrid_defaultDlStyle__969Qm DataGrid_asColumnUntilLg__ontpW'})
            dd_1 = li_div.find_all('dd',{'class' : 'DataGrid_defaultDdStyle__29SKf'})[0]
            dd_2 = li_div.find_all('dd',{'class' : 'DataGrid_defaultDdStyle__29SKf'})[1]
            dd_3 = li_div.find_all('dd',{'class' : 'DataGrid_defaultDdStyle__29SKf'})[2]
            dd_4 = li_div.find_all('dd',{'class' : 'DataGrid_defaultDdStyle__29SKf'})[3]
            li1 = dd_1.findAll('li')
            li2 = dd_2.findAll('li')
            li3 = dd_3.findAll('li')
            li4 = dd_4.findAll('li')
            
            
        except:
            #equipment_values= []
            litag = []
            #li= []



        lis1 = []
        lis2 = []
        lis3 = []
        lis4 = []

        try:
            dd_length1 = len(li1)
        except:
            dd_length1 = 1
            
        #equipment_lis = ("Electrical side mirrors")
        l = 1
        for l in range(dd_length1):
            try:    
                lis1.append(li1[l].get_text())
                
                l += 1  
            except:
                lis1.append('') #no_equipment_value
                
        try:
            dd_length2 = len(li2)
        except:
            dd_length2 = 1
            
            
        for l in range(dd_length2):
            try:    
                
                lis2.append(li2[l].get_text())
                
                l += 1  
            except:
                
                lis2.append('') #no_equipment_value
                
            
        try:
            dd_length3 = len(li3)
        except:
            dd_length3 = 1
            
            
        for l in range(dd_length3):
            try:    
                
                lis3.append(li3[l].get_text())
                
                l += 1  
            except:
                
                lis3.append('') #no_equipment_value
                
        try:
            dd_length4 = len(li4)
        except:
            dd_length4 = 1
            
            
        for l in range(dd_length4):
            try:    
                
                lis4.append(li4[l].get_text())
                
                l += 1  
            except:
                
                lis4.append('') #no_equipment_value
            
        #print("lis1:",lis1)
        #print("lis2:",lis2)
        #print("lis3:",lis3)
        #print("lis4:",lis4)


            #####################################################################################################################################

        #colour = ""

        try: 
            colour_class = ad_soup.find_all('dt',{'class' : 'DataGrid_defaultDtStyle__yzRR_'})
            
        except:
            colour_class = ""



        keyler = []

        try:
            len_dt_class=len(colour_class)
        except:
            len_dt_class=len(colour_class)
            

        l = 0
        for l in range(len_dt_class):
            try:    
                keyler.append(colour_class[l].get_text())
                    
            except:
                keyler.append('')

        #print("len_dt_class:",len_dt_class)
        #print("keyler:",keyler)


        # Store credantials in file my.propertiesans use Config parser to read from it

        config = configparser.RawConfigParser()
        config.read(filenames = 'my.properties')
        #print(config.sections())



        scrap_db = pymysql.connect(host='localhost',user='root',password='1234',database='autoscout',charset='utf8mb4',cursorclass=pymysql.cursors.DictCursor)

        cursor = scrap_db.cursor()


        # Drop table as per requirement

        #cursor.execute('DROP TABLE IF EXISTS CARS')

        # Create table as per requirement

        sql = """CREATE TABLE CARS(
            brand VARCHAR(32),
            model VARCHAR(32),
            model_version VARCHAR(64),
            location VARCHAR(64),
            price VARCHAR(32),
            dealer VARCHAR(32),
            contact_name VARCHAR(32),
            tel_number VARCHAR(32),
            mileage VARCHAR(32),
            gearbox VARCHAR(32),
            first_registration VARCHAR(7),
            fuel_type VARCHAR(64),
            power VARCHAR(16),
            seller VARCHAR(16),
            body_type VARCHAR(16),
            type VARCHAR(32),
            drivetrain VARCHAR(16),
            seats VARCHAR(8),
            doors VARCHAR(8),
            country_version VARCHAR(16),
            offer_number VARCHAR(16),
            model_code VARCHAR(16),
            production_date VARCHAR(16),
            general_inspection VARCHAR(16),
            previous_owner VARCHAR(8),
            full_service_history VARCHAR(8),
            non_smoker_vehicle VARCHAR(8),
            engine_size VARCHAR(16),
            gears VARCHAR(8),
            cylinders VARCHAR(8),
            fuel_consumption VARCHAR(64),
            CO2_emissions VARCHAR(32),
            power_consumption VARCHAR(32),
            energy_efficiency_class VARCHAR(8),
            CO2_efficiency VARCHAR(80),
            emission_class VARCHAR(16),
            emissions_sticker VARCHAR(16),
            last_service VARCHAR(16),
            last_timing_belt_change VARCHAR(16),
            empty_weightVARCHAR(16),
            colour_and_upholstery VARCHAR(64),
            all_equipment VARCHAR(2048),
            vehicle_description VARCHAR(4096),
            car_picture_link VARCHAR(256),
            ad_link VARCHAR(256),
            download_date_time VARCHAR(32),
            comfort_convenience VARCHAR(255),
            entertainment_media VARCHAR(255),  
            safety_security VARCHAR(255),
            extras VARCHAR(255),
            colour VARCHAR(16),
            paint VARCHAR(16),
            upholstery_colour VARCHAR(16),
            upholstery VARCHAR(16),
            manufacturer_colour VARCHAR(32)
            )"""

        cursor.execute(sql)   #Save data to the table

        
        

        for row_count in range(0, df_last.shape[0]):
            chunk = df_last.iloc[row_count:row_count + 1,:].values.tolist()
            
            #print("-------------------------------------------------------")
            #print(chunk[0])
            #print("-------------------------------------------------------")
            
            brand = ""
            model = ""
            model_version = ""
            location = ""
            price = ""
            dealer = ""
            contact_name = ""
            tel_number = ""
            mileage = ""
            gearbox = ""
            first_registration = ""
            fuel_type = ""
            power = ""
            seller = ""
            body_type = ""
            type = ""
            drivetrain = ""
            seats = ""
            doors = ""
            country_version = ""
            offer_number = ""
            model_code = ""
            production_date = ""
            general_inspection = ""
            previous_owner = ""
            full_service_history = ""
            non_smoker_vehicle = ""
            engine_size = ""
            gears = ""
            cylinders = ""
            fuel_consumption = ""
            CO2_emissions = ""
            power_consumption = ""
            energy_efficiency_class = ""
            CO2_efficiency = ""
            emission_class = ""
            emissions_sticker = ""
            last_service = ""
            last_timing_belt_change = ""
            empty_weight = ""
            colour_and_upholstery = ""
            all_equipment = ""
            vehicle_description = ""
            car_picture_link = ""
            ad_link = ""
            download_date_time= ""
            comfort_convenience= ""
            entertainment_media= ""
            safety_security= "" 
            extras= ""
            colour = ""
            paint = ""
            upholstery_colour = ""
            upholstery = ""
            manufacturer_colour = ""
            
            
            #control = "true"
            
            #i=0
            lenght_of_chunk = len(chunk[0])
            #print("lenght_of_chunk:",lenght_of_chunk)     # 24  for -> number = np.arange(2,3) # https://www.autoscout24.com/offers/audi-100-coupe-s-restaurationsfahrzeug-motor-laeuft-gasoline-red-f45e40a9-a46c-4b05-8699-9979e2c680f6
            
            
            if "brand" in cars_data:
                try:
                    brand = chunk[0][0]
                except:
                    brand = ""
            
            if "model" in cars_data:
                try:
                    model = chunk[0][1]
                except:
                    model = ""
            
            if "model_version" in cars_data:
                try:
                    model_version = chunk[0][2]
                except:
                    model_version = ""
            
            if "location" in cars_data:
                try:
                    location = chunk[0][3]
                except:
                    location = ""
            
            if "price" in cars_data:
                try:
                    price = chunk[0][4]
                except:
                    price = ""
            
            if "dealer" in cars_data:
                try:
                    dealer = chunk[0][5]
                except:
                    dealer = ""
            
            if "contact_name" in cars_data:
                try:
                    contact_name = chunk[0][6]
                except:
                    contact_name = ""
            
            if "tel_number" in cars_data:
                try:
                    tel_number = chunk[0][7]
                except:
                    tel_number = ""
            
            if "Mileage" in description_list:
                index_no = description_list.index("Mileage")   
                try:
                    mileage = value_list[index_no]
                except :
                    mileage = ""
            
            if "Gearbox" in description_list:
                index_no = description_list.index("Gearbox")   
                try:
                    gearbox = value_list[index_no]
                except :
                    gearbox = ""
            
            if "First_registration" in description_list:
                try:
                    first_registration = chunk[0][10]
                except:
                    first_registration = ""
            
            if "Fuel_type" in description_list:
                try:
                    fuel_type = chunk[0][11]
                except:
                    fuel_type = ""
                    
                    
            if "Power" in description_list:
                index_no = description_list.index("Power")   
                try:
                    power = value_list[index_no]
                except :
                    power = ""

            if "Seller" in description_list:
                try:
                    seller = chunk[0][13]
                except:
                    seller = ""
            
            if "Body_type" in all_key:
                index_no = all_key.index("Body_type")
                try:
                    body_type = all_value[index_no]  # index_no=0 olmali (burasi icin)
                except:
                    body_type = ""
                    
            if "Type" in all_key:
                index_no = all_key.index("Type")
                try:
                    type = all_value[index_no]
                except:
                    type = ""
                    
            if "Drivetrain" in all_key:
                index_no = all_key.index("Drivetrain")
                try:
                    drivetrain = all_value[index_no]
                except:
                    drivetrain = ""
                    
            if "Seats" in all_key:
                index_no = all_key.index("Seats")
                try:
                    seats = all_value[index_no]
                except:
                    seats = ""
                    
            if "Doors" in all_key:
                index_no = all_key.index("Doors")
                try:
                    doors = all_value[index_no]
                except:
                    doors = ""
                    
            if "Country_version" in all_key:
                index_no = all_key.index("Country_version")
                try:
                    country_version = all_value[index_no]
                except:
                    country_version = ""
                    
            if "Offer_number" in all_key:
                index_no = all_key.index("Offer_number")
                try:
                    offer_number = all_value[index_no]
                except:
                    offer_number = ""
                    
            if "Model_code" in all_key:
                index_no = all_key.index("Model_code")
                try:
                    model_code = all_value[index_no]
                except:
                    model_code = ""
                    
            if "Production_date" in all_key:
                index_no = all_key.index("Production_date")
                try:
                    production_date = all_value[index_no]
                except:
                    production_date = ""
                
            if "General_inspection" in all_key:
                index_no = all_key.index("General_inspection")
                try:
                    general_inspection = all_value[index_no]
                except:
                    general_inspection = ""
                    
            
            if "Previous_owner" in all_key:
                index_no = all_key.index("Previous_owner")
                try:
                    previous_owner = all_value[index_no]
                except :
                    previous_owner = ""
                    
            if "Full_service_history" in all_key:
                index_no = all_key.index("Full_service_history")
                try:
                    full_service_history = all_value[index_no] # yes or no
                except :
                    full_service_history = ""
                    
            if "Non_smoker_vehicle" in all_key:
                index_no = all_key.index("Non_smoker_vehicle")
                try:
                    non_smoker_vehicle = all_value[index_no] # yes or no
                except :
                    non_smoker_vehicle = ""
                    
            if "Engine_size" in all_key:
                index_no = all_key.index("Engine_size")
                try:
                    engine_size = all_value[index_no]
                    #for entry in chunk[0]:
                    #    if entry.endswith("cc"):
                    #        engine_size = entry       # chunk[0].endswith('cc') in chunk[0] # cc
                except :
                    engine_size = ""
                    
            if "Gears" in all_key:
                index_no = all_key.index("Gears")
                try:
                    gears = all_value[index_no]
                except :
                    gears = ""
                    
            if "Cylinders" in all_key:
                index_no = all_key.index("Cylinders")
                try:
                    cylinders = all_value[index_no]
                except :
                    cylinders = ""        
            
            if "Fuel_consumption" in all_key:
                index_no = all_key.index("Fuel_consumption")
                try:
                    fuel_consumption = all_value[index_no]
                except :
                    fuel_consumption = ""        
            
            if "CO2_emissions" in all_key:
                index_no = all_key.index("CO2_emissions")
                try:
                    CO2_emissions = all_value[index_no]
                except :
                    CO2_emissions = ""        
                    
            
            if "Power_consumption" in all_key:
                index_no = all_key.index("Power_consumption")
                try:
                    power_consumption = all_value[index_no]
                except :
                    power_consumption = "" 
            
            if "Energy_efficiency_class" in all_key:
                index_no = all_key.index("Energy_efficiency_class")
                try:
                    energy_efficiency_class = all_value[index_no]
                except :
                    energy_efficiency_class = ""        
            
            if "CO2_efficiency" in all_key:
                index_no = all_key.index("CO2_efficiency")
                try:
                    CO2_efficiency = all_value[index_no]
                except :
                    CO2_efficiency = ""        
            
            if "Emission_class" in all_key:
                index_no = all_key.index("Emission_class")
                try:
                    emission_class = all_value[index_no]
                except :
                    emission_class = ""        
            
            if "Emissions_sticker" in all_key:
                index_no = all_key.index("Emissions_sticker")
                try:
                    emissions_sticker = all_value[index_no]
                except :
                    emissions_sticker = ""
            
            if "Last_service" in all_key:
                index_no = all_key.index("Last_service")
                try:
                    last_service = all_value[index_no]
                except :
                    last_service = ""   
            
            if "Last_timing_belt_change" in all_key:
                index_no = all_key.index("Last_timing_belt_change")
                try:
                    last_timing_belt_change = all_value[index_no]
                except :
                    last_timing_belt_change = ""   
            
            if "Empty_weight" in all_key:
                index_no = all_key.index("Empty_weight")
                try:
                    empty_weight = all_value[index_no]
                except :
                    empty_weight = "" 
            
            if "Colour" in keyler:
                index_no = keyler.index("Colour")
                try:
                    colour_and_upholstery = colour_and_upholstery+"-"+all_value[index_no-len_equipment_class]
                except:
                    pass
                    
            if "Paint" in keyler:
                index_no = keyler.index("Paint")
                try:
                    colour_and_upholstery = colour_and_upholstery+"-"+all_value[index_no-len_equipment_class]
                except:
                    pass
                    
            if "Upholstery colour" in keyler:
                index_no = keyler.index("Upholstery colour")
                try:
                    colour_and_upholstery = colour_and_upholstery+"-"+all_value[index_no-len_equipment_class]
                except:
                    pass
                    
            if "Upholstery" in keyler:
                index_no = keyler.index("Upholstery")
                try:
                    colour_and_upholstery = colour_and_upholstery+"-"+all_value[index_no-len_equipment_class]
                except:
                    pass
            
            if "Manufacturer colour" in keyler:
                index_no = keyler.index("Manufacturer colour")
                try:
                    colour_and_upholstery = colour_and_upholstery+"-"+all_value[index_no-len_equipment_class]
                except:
                    pass
            
            
                    
            if "Comfort & Convenience" in equipmentkeyfirst:
                len_lis1 = len(lis1)
                for l in range(len_lis1):
                
                    try:
                        comfort_convenience +="-"+lis1[l]   
                        #l += 1
                        
                    except:
                        comfort_convenience = ""    
            
            if "Entertainment & Media" in quipmentkeysecond:
                len_lis2 = len(lis2)
                for l in range(len_lis2):
                
                    try:
                        entertainment_media +="-"+lis2[l]
                        #l += 1
                        
                    except:
                        entertainment_media = ""    
            
            if "Safety & Security" in quipmentkeythird:
                len_lis3 = len(lis3)
                for l in range(len_lis3):
                
                    try:
                        safety_security +="-"+lis3[l]
                        #l += 1
                        
                    except:
                        safety_security = ""    
            
            if "Extras" in quipmentkeyfourth:
                len_lis4 = len(lis4)
                for l in range(len_lis4):
                
                    try:
                        extras +="-"+lis4[l]
                        #l += 1
                        
                    except:
                        extras = ""    
                        
            if "Colour" in keyler:
                index_no = keyler.index("Colour")
                try:
                    colour = all_value[index_no-len_equipment_class]
                except:
                    colour = ""
                    
            #print("len_dt_class-len_equipment_class:",len_dt_class-len_equipment_class-len_equipment_class)
            #print("index_on_colour:",index_no)
            #print("colour:",colour)
            #########################
            if "Paint" in keyler:
                index_no = keyler.index("Paint")
                try:
                    paint = all_value[index_no-len_equipment_class]
                except:
                    paint = ""
            
            #print("paint:",paint)
            ########################
            
            if "Upholstery colour" in keyler:
                index_no = keyler.index("Upholstery colour")
                try:
                    upholstery_colour = all_value[index_no-len_equipment_class]
                except:
                    upholstery_colour = ""
            
            #print("Upholstery colour:",upholstery_colour)
            ########################
            
            if "Upholstery" in keyler:
                index_no = keyler.index("Upholstery")
                try:
                    upholstery = all_value[index_no-len_equipment_class]
                except:
                    upholstery = ""
            
            #print("Upholstery:",upholstery)
            ########################
            
            if "Manufacturer colour" in keyler:
                index_no = keyler.index("Manufacturer colour")
                try:
                    manufacturer_colour = all_value[index_no-len_equipment_class]
                except:
                    manufacturer_colour = ""
            
                
            if chunk[0][lenght_of_chunk-5] != "":
                all_equipment = chunk[0][lenght_of_chunk-5] # all_equipment
            
            
            #####################################################
                
            if chunk[0][lenght_of_chunk-4] != "":
                vehicle_description = chunk[0][lenght_of_chunk-4] # vehicle_description
                
            if chunk[0][lenght_of_chunk-3] != "":
                car_picture_link = chunk[0][lenght_of_chunk-3] # car_picture_link
                
            if chunk[0][lenght_of_chunk-2] != "":
                ad_link = chunk[0][lenght_of_chunk-2]  # ad_link
                
            if chunk[0][lenght_of_chunk-1] != "":
                download_date_time = chunk[0][lenght_of_chunk-1]  # datetime_string
                
        #print("-------------------------------------------------------")
        #print(brand)
        #print("-------------------------------------------------------")
        
        if (brand == ' '):
            control = "false"
        else:
            control = "true"

        if control == "true":
            mySql_insert_query = "INSERT INTO CARS (brand,model,model_version,location,price,dealer,contact_name,tel_number,mileage,gearbox,first_registration,fuel_type,power,seller,body_type,type,drivetrain,seats,doors,country_version,offer_number,model_code,production_date,general_inspection,previous_owner,full_service_history,non_smoker_vehicle,engine_size,gears,cylinders,fuel_consumption,CO2_emissions,power_consumption,energy_efficiency_class,CO2_efficiency,emission_class,emissions_sticker,last_service,last_timing_belt_change,empty_weight,colour_and_upholstery,all_equipment,vehicle_description,car_picture_link,ad_link,download_date_time,comfort_convenience,entertainment_media,safety_security,extras,colour,paint,upholstery_colour,upholstery,manufacturer_colour) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val =                                  (brand,model,model_version,location,price,dealer,contact_name,tel_number,mileage,gearbox,first_registration,fuel_type,power,seller,body_type,type,drivetrain,seats,doors,country_version,offer_number,model_code,production_date,general_inspection,previous_owner,full_service_history,non_smoker_vehicle,engine_size,gears,cylinders,fuel_consumption,CO2_emissions,power_consumption,energy_efficiency_class,CO2_efficiency,emission_class,emissions_sticker,last_service,last_timing_belt_change,empty_weight,colour_and_upholstery,all_equipment,vehicle_description,car_picture_link,ad_link,download_date_time,comfort_convenience,entertainment_media,safety_security,extras,colour,paint,upholstery_colour,upholstery,manufacturer_colour)

            #cursor = scrap_db.cursor()
            cursor.execute(mySql_insert_query, val) # cursor.executemany(mySql_insert_query, tuple_of_tuples)
            
            scrap_db.commit()
            print(cursor.rowcount, "Record inserted successfully into *CARS* table")

            #Disconnect from server
            #scrap_db.close()   
            
        driver.close()
    
if __name__ == '__main__':
    with concurrent.futures.ProcessPoolExecutor() as executor:  # ThreadPoolExecutor
        i = list(range(x,y))    # i = [x:y]
        executor.map(fonksyion,i)


#fonksyion(0,100)
