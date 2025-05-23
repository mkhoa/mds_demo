import time
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException

options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

url = ["https://winmart.vn/info/danh-sach-cua-hang/thanh-pho-ho-chi-minh",
"https://winmart.vn/info/danh-sach-cua-hang/thanh-pho-ha-noi",
"https://winmart.vn/info/danh-sach-cua-hang/thanh-pho-da-nang/"]

@data_loader
def load_data(*args, **kwargs):

    df = pd.DataFrame()
    for i in url:
        results = crawl_winmart(i)
        df = pd.concat([df, results], ignore_index=True)

    return df

def crawl_winmart(url):
    """
    Crawl winmart store location
    
    """
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)
    ## Input into search box
    # dropdown_box = driver.find_element(By.XPATH, "//input[@placeholder='Tỉnh thành']")
    # dropdown_box.send_keys(Keys.CONTROL + "A")
    # dropdown_box.send_keys("TP. Hà Nội")
    # time.sleep(1)
    # dropdown_box.send_keys(Keys.ARROW_DOWN)
    # time.sleep(1)
    # dropdown_box.send_keys(Keys.RETURN)
    # time.sleep(1)
    
    SCROLL_PAUSE_TIME = 3
    i = 0
    last_height = driver.execute_script("return document.body.scrollHeight")
    
    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(SCROLL_PAUSE_TIME)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        i += 1
        if i == 15:
            break
    
    WebDriverWait(driver, 3)
    elem = driver.find_elements(By.XPATH, "//div[contains(@class, 'shop-liststyles__ShopCard-sc-w4fccm-2')]");

    # dictionary with list object in values
    df = pd.DataFrame()

    results = {
        'Store_Name' : '',
        'Store_Address' : '',
        'Source_url': '',
    }
     
    for e in elem:
        results['Store_Name'] = e.find_element(By.XPATH, ".//div[1]").text
        results['Store_Address'] = e.find_element(By.XPATH, ".//div[2]").text
        results['Source_url'] = url
        df_new_row = pd.DataFrame(results, index=[0])
        df = pd.concat([df, df_new_row], ignore_index=True)

    df['Store_Chain'] = 'Winmart'
    driver.close()

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'