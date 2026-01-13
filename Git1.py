import time
import pandas as pd
import os
import datetime
import re
import random
import yaml
import json
import smtplib
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage

# --- SELENIUM STANDARD IMPORTS ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, StaleElementReferenceException, ElementClickInterceptedException, NoSuchElementException
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager # ใช้จัดการ Driver อัตโนมัติ

from dotenv import load_dotenv
from thefuzz import fuzz 
from dateutil.relativedelta import relativedelta 
import logging
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, TaskProgressColumn
from rich.theme import Theme

# --- SETUP & CONFIG ---
try:
    from fake_useragent import UserAgent
except ImportError:
    UserAgent = None

logging.getLogger("WDM").setLevel(logging.CRITICAL) # ปิด Log ของ Webdriver Manager

ENV_PATH = "User.env"
COMPETITORS_PATH = "compe.yaml"
CLIENTS_PATH = "co.yaml"
TIER1_PATH = "tier1.yaml"
RESUME_IMAGE_FOLDER = "resume_images" 
EMAIL_USE_HISTORY = False        

rec_env = os.getenv("EMAIL_RECEIVER")
MANUAL_EMAIL_RECEIVERS = [rec_env] if rec_env else []

custom_theme = Theme({"info": "dim cyan", "warning": "yellow", "error": "bold red", "success": "bold green"})
console = Console(theme=custom_theme)

load_dotenv(ENV_PATH, override=True)
MY_USERNAME = os.getenv("JOBTHAI_USER")
MY_PASSWORD = os.getenv("JOBTHAI_PASS")

G_SHEET_KEY_JSON = os.getenv("G_SHEET_KEY")
G_SHEET_NAME = os.getenv("G_SHEET_NAME")

TIER1_TARGETS = {}
if os.path.exists(TIER1_PATH):
    try:
        with open(TIER1_PATH, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)
            if yaml_data:
                for k, v in yaml_data.items():
                    if v:
                        if isinstance(v, list): TIER1_TARGETS[k] = [str(x).strip() for x in v]
                        else: TIER1_TARGETS[k] = [str(v).strip()]
    except Exception as e: console.print(f"⚠️ Load Tier1 Error: {e}", style="yellow")

TARGET_COMPETITORS_TIER2 = [] 
if os.path.exists(COMPETITORS_PATH):
    try:
        with open(COMPETITORS_PATH, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)
            if yaml_data and 'competitors' in yaml_data:
                TARGET_COMPETITORS_TIER2 = [str(x).strip() for x in yaml_data['competitors'] if x]
    except: pass

CLIENTS_TARGETS = {}
if os.path.exists(CLIENTS_PATH):
    try:
        with open(CLIENTS_PATH, "r", encoding="utf-8") as f:
            CLIENTS_TARGETS = yaml.safe_load(f) or {}
            for k in list(CLIENTS_TARGETS.keys()):
                if not CLIENTS_TARGETS[k]: del CLIENTS_TARGETS[k]
                elif not isinstance(CLIENTS_TARGETS[k], list): CLIENTS_TARGETS[k] = [str(CLIENTS_TARGETS[k])]
    except: pass

TARGET_UNIVERSITIES = ["วไลยอลงกรณ์", "Valaya Alongkorn Rajabhat University under the Royal Patronage"]  
TARGET_FACULTIES = ["เครื่องสำอาง","Cosmetic Science"] 
TARGET_MAJORS = ["เครื่องสำอาง", "วิทยาศาสตร์เครื่องสำอาง","Cosmetic Science", "Cosmetics", "Cosmetic"]
SEARCH_KEYWORDS = ["วไลยอลงกรณ์ เครื่องสำอาง","Cosmetic Valaya Alongkorn"]

KEYWORDS_CONFIG = {
    "NPD": {"titles": ["NPD", "R&D", "RD", "Research", "Development", "วิจัย", "พัฒนา", "Formulation", "สูตร"]},
    "PCM": {"titles": ["PCM", "Production", "ผลิต", "Manufacturing", "Factory", "โรงงาน", "QA", "QC"]},
    "Sales": {"titles": ["Sale", "Sales", "ขาย", "AE", "BD", "Customer", "Telesale"]},
    "MKT": {"titles": ["MKT", "Marketing", "การตลาด", "Digital", "Content", "Media", "Ads"]},
    "Admin": {"titles": ["Admin", "ธุรการ", "ประสานงาน", "Coordinator", "Document", "เอกสาร"]},
    "HR": {"titles": ["HR", "Recruit", "สรรหา", "บุคคล", "Training", "Payroll"]},
    "SCM": {"titles": ["SCM", "Supply Chain", "Logistic", "ขนส่ง", "Warehouse", "Stock", "Import", "Export"]},
    "PUR": {"titles": ["PUR", "Purchase", "จัดซื้อ", "Sourcing", "Buyer"]},
    "DATA": {"titles": ["Data", "ข้อมูล", "Analyst", "Statistic", "สถิติ"]},
    "Present": {"titles": ["Present", "Speaker", "วิทยากร", "Trainer"]},
    "IT": {"titles": ["IT", "Computer", "Software", "Programmer", "Developer"]},
    "RA": {"titles": ["RA", "Regulatory", "อย.", "FDA", "ขึ้นทะเบียน"]},
    "ACC": {"titles": ["ACC", "Account", "บัญชี", "Finance", "การเงิน", "Audit"]}
}

class JobThaiRowScraper:
    def __init__(self):
        console.rule("[bold cyan]🛡️ JobThai Scraper (GitHub Actions Robust Edition)[/]")
        self.history_file = "notification_history_uni.json" 
        self.history_data = {}
        
        # Safe Directory Creation
        if not os.path.exists(RESUME_IMAGE_FOLDER): 
            try: os.makedirs(RESUME_IMAGE_FOLDER, exist_ok=True)
            except: pass
        
        if EMAIL_USE_HISTORY and os.path.exists(self.history_file):
            try:
                with open(self.history_file, 'r', encoding='utf-8') as f: self.history_data = json.load(f)
            except: self.history_data = {}

        if UserAgent: self.ua = UserAgent(browsers=['chrome'], os=['windows'])
        else: self.ua = None

        # --- 🟢 DRIVER CONFIG FOR GITHUB ACTIONS (SELENIUM STANDARD) ---
        opts = webdriver.ChromeOptions()
        
        # Headless Config
        opts.add_argument('--headless=new') # บังคับ Headless บน GitHub
        opts.add_argument('--window-size=1920,1080')
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        
        # Anti-Detection & Popup
        opts.add_argument("--disable-popup-blocking")
        opts.add_argument("--disable-notifications")
        opts.add_argument("--lang=th-TH")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        
        # Fake User-Agent (Critical for JobThai)
        fake_user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        opts.add_argument(f'--user-agent={fake_user_agent}')
        
        try:
            # ใช้ ChromeDriverManager เพื่อโหลด Driver ให้ตรงกับ Chrome บน GitHub Actions อัตโนมัติ
            self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
        except Exception as e:
            console.print(f"❌ Driver Init Error: {e}", style="error")
            raise e
        
        self.driver.set_page_load_timeout(60) 
        self.wait = WebDriverWait(self.driver, 20)
        self.total_profiles_viewed = 0 
        self.all_scraped_data = []

    def set_random_user_agent(self):
        if self.ua:
            try: self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": self.ua.random})
            except: pass

    def random_sleep(self, min_t=2.0, max_t=5.0): 
        time.sleep(random.uniform(min_t, max_t))

    def wait_for_page_load(self, timeout=15):
        try:
            WebDriverWait(self.driver, timeout).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
        except: pass

    def safe_click(self, selector, by=By.XPATH, timeout=10):
        # ... (ใช้ Logic เดิมของคุณ เพราะดีอยู่แล้ว) ...
        try:
            element = WebDriverWait(self.driver, timeout).until(EC.element_to_be_clickable((by, selector)))
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            time.sleep(0.5)
            element.click()
            return True
        except:
            try:
                element = self.driver.find_element(by, selector)
                self.driver.execute_script("arguments[0].click();", element)
                return True
            except: return False
    
    def human_scroll(self):
        try:
            total_height = self.driver.execute_script("return document.body.scrollHeight")
            current_position = 0
            while current_position < total_height:
                scroll_step = random.randint(300, 700)
                current_position += scroll_step
                self.driver.execute_script(f"window.scrollTo(0, {current_position});")
                time.sleep(random.uniform(0.1, 0.4))
            self.driver.execute_script("window.scrollTo(0, 0);")
        except: pass

    def parse_thai_date_exact(self, date_str):
        # ... (Logic เดิม) ...
        if not date_str: return None
        thai_months = {'มกราคม': 1, 'กุมภาพันธ์': 2, 'มีนาคม': 3, 'เมษายน': 4, 'พฤษภาคม': 5, 'มิถุนายน': 6, 'กรกฎาคม': 7, 'สิงหาคม': 8, 'กันยายน': 9, 'ตุลาคม': 10, 'พฤศจิกายน': 11, 'ธันวาคม': 12}
        try:
            date_str = date_str.strip()
            parts = date_str.split() 
            if len(parts) < 3: return None
            day = int(parts[0])
            month = thai_months.get(parts[1])
            year_be = int(parts[2])
            year_ad = year_be - 543
            return datetime.date(year_ad, month, day)
        except: return None

    # ==============================================================================
    # 🔥 STEP 1: ROBUST LOGIN (NO COOKIE, NO HAMMER)
    # ==============================================================================
    def step1_login(self):
        login_url = "https://www.jobthai.com/th/employer"
        max_retries = 5 
        
        for attempt in range(1, max_retries + 1):
            console.rule(f"[bold cyan]🔐 Login Attempt {attempt}/{max_retries}[/]")
            try:
                self.driver.get(login_url)
                self.wait_for_page_load()
                
                # 1. Kill Popups / PDPA
                console.print("   🧹 Clearing Popups...", style="dim")
                try:
                    self.driver.execute_script("""
                        var pdpa = document.querySelector('#pdpa-consent-dialog button');
                        if(pdpa) pdpa.click();
                        var modal = document.querySelector('.modal-close-btn');
                        if(modal) modal.click();
                    """)
                    time.sleep(1)
                except: pass

                # 2. Force Switch Tab to Employer (JS Injection)
                console.print("   ⚡ Injecting JS to switch tab...", style="info")
                try:
                    self.driver.execute_script("""
                        document.querySelectorAll('li[data-tab]').forEach(el => el.classList.remove('active'));
                        var tabEmp = document.querySelector('li[data-tab="employer"]');
                        if(tabEmp) tabEmp.classList.add('active');
                        
                        var formSeeker = document.getElementById('login-form-jobseeker');
                        if(formSeeker) formSeeker.style.display = 'none';
                        
                        var formEmp = document.getElementById('login-form-employer');
                        if(formEmp) formEmp.style.display = 'block';
                    """)
                    time.sleep(1)
                except Exception as e: console.print(f"⚠️ Tab switch warning: {e}", style="dim")

                # 3. Fill Username/Password
                console.print("   📝 Inputting credentials...", style="info")
                
                # รอให้เจอช่อง Username และต้อง interactable
                user_inp = WebDriverWait(self.driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#login-form-username, input[name='username']"))
                )
                # ใช้ ActionChains เพื่อความชัวร์
                ActionChains(self.driver).move_to_element(user_inp).click().perform()
                user_inp.clear()
                user_inp.send_keys(MY_USERNAME)
                
                pass_inp = self.driver.find_element(By.CSS_SELECTOR, "#login-form-password, input[name='password']")
                pass_inp.clear()
                pass_inp.send_keys(MY_PASSWORD)
                
                # 4. Submit
                console.print("   🚀 Submitting...", style="info")
                pass_inp.send_keys(Keys.ENTER)
                time.sleep(2)
                
                # ถ้ากด Enter ไม่ไป ให้ลอง Click ปุ่ม
                try:
                    btn = self.driver.find_element(By.CSS_SELECTOR, "#btn-login, button[type='submit']")
                    self.driver.execute_script("arguments[0].click();", btn)
                except: pass

                # 5. Verify Login
                console.print("   ⏳ Verifying...", style="dim")
                try:
                    WebDriverWait(self.driver, 20).until(
                        lambda d: "login" not in d.current_url or "findresume" in d.current_url
                    )
                    console.print(f"🎉 Login Success! (Attempt {attempt})", style="bold green")
                    return True
                except TimeoutException:
                    # Check Error Message
                    try:
                        err = self.driver.find_element(By.CSS_SELECTOR, ".error-message").text
                        console.print(f"   🛑 Login Error Message: {err}", style="bold red")
                    except: pass
                    console.print("   ❌ Timeout: URL didn't change.", style="error")

            except Exception as e:
                console.print(f"   ⚠️ Exception: {e}", style="warning")
                try: self.driver.delete_all_cookies()
                except: pass
                
        console.print("❌ Login Failed after all attempts.", style="bold red")
        return False

    def step2_search(self, keyword):
        search_url = "https://www3.jobthai.com/findresume/findresume.php?l=th"
        console.print(f"2️⃣   ค้นหา: '[bold]{keyword}[/]' ...", style="info")
        
        try:
            self.driver.get(search_url)
            self.wait_for_page_load()
            
            # รอช่อง Search
            kw_element = WebDriverWait(self.driver, 20).until(
                EC.presence_of_element_located((By.ID, "KeyWord"))
            )
            
            # ใช้ JS พิมพ์เพื่อความแม่นยำ
            self.driver.execute_script("arguments[0].value = arguments[1];", kw_element, keyword)
            
            # กด Search
            search_btn = self.driver.find_element(By.ID, "buttonsearch")
            self.driver.execute_script("arguments[0].click();", search_btn)
            
            console.print("   🔍 รอผลลัพธ์...", style="dim")
            
            # รอผลลัพธ์
            try:
                WebDriverWait(self.driver, 15).until(
                    lambda d: "ResumeDetail" in d.page_source or "ไม่พบข้อมูล" in d.page_source or "No data found" in d.page_source
                )
            except TimeoutException:
                console.print("   ⚠️ Search Timeout (อาจจะเน็ตช้า)", style="warning")
                return False

            src = self.driver.page_source
            if "ไม่พบข้อมูล" in src or "No data found" in src:
                console.print(f"   ⚠️ ไม่พบข้อมูล (0 Results)", style="warning")
                return True # ถือว่าสำเร็จแต่ไม่มีของ
            
            console.print(f"   ✅ เจอผลการค้นหา!", style="success")
            return True

        except Exception as e:
            console.print(f"❌ Search Error ({keyword}): {e}", style="error")
            return False

    def step3_collect_all_links(self):
        collected_links = []
        page_num = 1
        console.rule("[bold yellow]3️⃣  โหมดเก็บลิงก์[/]")
        
        while True:
            console.print(f"   📄 หน้าที่ {page_num}...", style="info")
            try:
                # รอลิงก์ Resume
                try:
                    WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.XPATH, "//a[contains(@href, 'ResumeDetail')]")))
                except: pass
                
                all_anchors = self.driver.find_elements(By.XPATH, "//a[contains(@href, 'ResumeDetail') or contains(@href, '/resume/')]")
                
                count_before = len(collected_links)
                for a in all_anchors:
                    try:
                        href = a.get_attribute("href")
                        if href and href not in collected_links:
                            collected_links.append(href)
                    except: continue
                
                new_count = len(collected_links) - count_before
                console.print(f"      -> เก็บเพิ่ม: {new_count} (รวม {len(collected_links)})", style="success")

            except Exception as e:
                console.print(f"      ❌ Error เก็บลิงก์: {e}", style="error")

            if len(collected_links) == 0: break
            if new_count == 0: break

            try:
                # หาปุ่ม Next (ลองหลาย selector)
                next_btns = self.driver.find_elements(By.XPATH, '//*[@id="content-l"]/div[2]/div[1]/table/tbody/tr/td[8]/a')
                found = False
                for btn in next_btns:
                    if "ถัดไป" in btn.text or "Next" in btn.text or ">" in btn.text:
                        self.driver.execute_script("arguments[0].click();", btn)
                        found = True
                        break
                
                if not found and next_btns:
                     self.driver.execute_script("arguments[0].click();", next_btns[0])
                     found = True

                if found:
                    page_num += 1
                    time.sleep(3)
                else: break
            except: break
            
        console.print(f"[bold green]📦 สรุปยอดรวม: {len(collected_links)} ลิงก์[/]")
        return collected_links

    # ==============================================================================
    # 🕵️ DATA EXTRACTION (ข้อมูลครบถ้วนตามเดิม)
    # ==============================================================================
    def scrape_detail_from_json(self, url, keyword, progress_console=None):
        printer = progress_console if progress_console else console
        self.set_random_user_agent()
        
        load_success = False
        for _ in range(3):
            try:
                self.driver.get(url)
                self.wait_for_page_load()
                load_success = True
                break 
            except: self.random_sleep(5, 10)

        if not load_success: return None, 999, None
        
        try: self.human_scroll() 
        except: pass
        self.random_sleep(2.0, 4.0)
        
        data = {'Link': url}
        try: full_text = self.driver.find_element(By.CSS_SELECTOR, "#mainTableTwoColumn").text
        except: full_text = ""
        
        def get_val(sel, xpath=False):
            try:
                elem = self.driver.find_element(By.XPATH, sel) if xpath else self.driver.find_element(By.CSS_SELECTOR, sel)
                return elem.text.strip()
            except: return ""

        # --- 🎓 Degree Parsing ---
        edu_tables_xpath = '//*[@id="mainTableTwoColumn"]/tbody/tr/td[1]/table/tbody/tr[7]/td[2]/table'
        try:
            edu_tables = self.driver.find_elements(By.XPATH, edu_tables_xpath)
            total_degrees = len(edu_tables)
        except: total_degrees = 0
        
        matched_uni = ""; matched_faculty = ""; matched_major = ""; is_qualified = False
        highest_degree_text = "-"; max_degree_score = -1
        degree_score_map = {"ปริญญาเอก": 3, "ดุษฎีบัณฑิต": 3, "Doctor": 3, "Ph.D": 3, "ปริญญาโท": 2, "มหาบัณฑิต": 2, "Master": 2, "ปริญญาตรี": 1, "บัณฑิต": 1, "Bachelor": 1}
        
        def check_fuzzy(scraped_text, target_list, threshold=85):
            if not target_list: return True
            if not scraped_text: return False
            best_score = 0
            for target in target_list:
                score = fuzz.partial_ratio(target.lower(), scraped_text.lower())
                if score > best_score: best_score = score
            return best_score >= threshold

        for i in range(1, total_degrees + 1):
            base_xpath = f'//*[@id="mainTableTwoColumn"]/tbody/tr/td[1]/table/tbody/tr[7]/td[2]/table[{i}]'
            curr_uni = get_val(f'{base_xpath}/tbody/tr[2]/td/div', True)
            if not curr_uni: curr_uni = get_val(f'{base_xpath}/tbody/tr[1]/td/div', True)
            
            curr_degree = get_val(f'{base_xpath}//td[contains(., "ระดับการศึกษา")]/following-sibling::td[1]', True)
            if not curr_degree: curr_degree = get_val(f'{base_xpath}/tbody/tr[1]/td', True)
            
            curr_faculty = get_val(f'{base_xpath}//td[contains(., "คณะ")]/following-sibling::td[1]', True)
            curr_major = get_val(f'{base_xpath}//td[contains(., "สาขา")]/following-sibling::td[1]', True)
            
            score = 0
            for key, val in degree_score_map.items():
                if key in str(curr_degree): score = val; break
            if score > max_degree_score: max_degree_score = score; highest_degree_text = curr_degree
            elif score == max_degree_score and highest_degree_text == "-": highest_degree_text = curr_degree

            if not is_qualified:
                uni_pass = check_fuzzy(curr_uni, TARGET_UNIVERSITIES)
                fac_pass = check_fuzzy(curr_faculty, TARGET_FACULTIES)
                major_pass = check_fuzzy(curr_major, TARGET_MAJORS)
                if uni_pass and (fac_pass or major_pass):
                    is_qualified = True; matched_uni = curr_uni; matched_faculty = curr_faculty; matched_major = curr_major

        if not is_qualified: return None, 999, None # Filter Logic
        
        data['ระดับการศึกษา'] = highest_degree_text; data['มหาลัย'] = matched_uni; data['คณะ'] = matched_faculty; data['สาขา'] = matched_major
        data['รหัสใบสมัคร'] = get_val("#ResumeViewDiv [align='left'] span.white")
        
        # --- 🖼️ Image Saving (Safe Mode) ---
        try:
            img_element = self.driver.find_element(By.ID, "DefaultPictureResume2Column")
            app_id_clean = data['รหัสใบสมัคร'].strip() if data['รหัสใบสมัคร'] else f"unknown_{int(time.time())}"
            img_filename = f"{app_id_clean}.png"
            if not os.path.exists(RESUME_IMAGE_FOLDER): 
                try: os.makedirs(RESUME_IMAGE_FOLDER, exist_ok=True)
                except: pass
            
            if os.path.exists(RESUME_IMAGE_FOLDER):
                save_path = os.path.join(RESUME_IMAGE_FOLDER, img_filename)
                img_element.screenshot(save_path)
                data['รูปภาพ'] = save_path
            else: data['รูปภาพ'] = ""
        except: data['รูปภาพ'] = ""

        raw_update_date = get_val('//*[@id="ResumeViewDiv"]/table/tbody/tr[2]/td[3]/span[2]', xpath=True)
        
        def calculate_last_update(date_str):
            if not date_str: return "-"
            try:
                parts = date_str.split()
                if len(parts) < 3: return "-"
                day = int(parts[0])
                thai_months = {'มกราคม': 1, 'กุมภาพันธ์': 2, 'มีนาคม': 3, 'เมษายน': 4, 'พฤษภาคม': 5, 'มิถุนายน': 6, 'กรกฎาคม': 7, 'สิงหาคม': 8, 'กันยายน': 9, 'ตุลาคม': 10, 'พฤศจิกายน': 11, 'ธันวาคม': 12}
                month = thai_months.get(parts[1], 1)
                year_ad = int(parts[2]) - 543
                update_dt = datetime.datetime(year_ad, month, day)
                diff = relativedelta(datetime.datetime.now(), update_dt)
                txt = []
                if diff.years > 0: txt.append(f"{diff.years}ปี")
                if diff.months > 0: txt.append(f"{diff.months}เดือน")
                if diff.days > 0: txt.append(f"{diff.days}วัน")
                return " ".join(txt) if txt else "วันนี้"
            except: return "-"
            
        data['อัพเดทล่าสุด'] = calculate_last_update(raw_update_date)
        data['ชื่อ'] = get_val("#mainTableTwoColumn td > span.head1")
        data['นามสกุล'] = get_val("span.black:nth-of-type(3)")
        
        age_match = re.search(r"อายุ\s*[:]?\s*(\d+)", full_text)
        data['อายุ'] = age_match.group(1) if age_match else ""
        data['เพศ'] = re.search(r"เพศ\s*[:]?\s*(ชาย|หญิง|Male|Female)", full_text).group(1) if re.search(r"เพศ", full_text) else ""
        data['เบอร์โทร'] = get_val("#mainTableTwoColumn div:nth-of-type(6) span.black")
        data['Email'] = get_val("#mainTableTwoColumn a")
        data['ที่อยู่'] = get_val("#mainTableTwoColumn div:nth-of-type(1) span.head1")
        data['จังหวัดที่อยู่'] = get_val("#mainTableTwoColumn table [width][align='left'] div span.headNormal")
        
        pos1 = get_val('//*[@id="mainTableTwoColumn"]/tbody/tr/td[1]/table/tbody/tr[5]/td[2]/table/tbody/tr[3]/td/span[2]', xpath=True)
        pos2 = get_val('//*[@id="mainTableTwoColumn"]/tbody/tr/td[1]/table/tbody/tr[5]/td[2]/table/tbody/tr[3]/td/span[4]', xpath=True)
        pos3 = get_val('//*[@id="mainTableTwoColumn"]/tbody/tr/td[1]/table/tbody/tr[5]/td[2]/table/tbody/tr[3]/td/span[6]', xpath=True)
        data['ตำแหน่งที่ต้องการสมัคร_1'] = pos1; data['ตำแหน่งที่ต้องการสมัคร_2'] = pos2; data['ตำแหน่งที่ต้องการสมัคร_3'] = pos3
        combined_positions = ", ".join([p for p in [pos1, pos2, pos3] if p])
        data['เงินเดือนที่ต้องการ'] = get_val("//td[contains(., 'เงินเดือนที่ต้องการ')]/following-sibling::td[1]", True)
        
        salary_min_txt = "-"; salary_max_txt = "-"
        raw_salary = data.get('เงินเดือนที่ต้องการ', '')
        try:
            if raw_salary and 'ปิดข้อมูล' not in str(raw_salary):
                s = str(raw_salary).lower().replace(',', '')
                s = re.sub(r'(\d+(\.\d+)?)\s*k', lambda m: str(float(m.group(1)) * 1000), s)
                nums = [float(n) for n in re.findall(r'\d+(?:\.\d+)?', s)]
                if nums:
                    mn, mx = nums[0], nums[0]
                    if len(nums) >= 2: mn, mx = nums[0], nums[1]
                    if mx > 1000 and mn < 1000 and mn > 0: mn *= 1000
                    salary_min_txt = f"{int(mn):,}"; salary_max_txt = f"{int(mx):,}"
        except: pass
        data['Salary_Min'] = salary_min_txt; data['Salary_Max'] = salary_max_txt

        all_work_history = []
        try:
            if "ประวัติการทำงาน/ฝึกงาน" in full_text:
                history_text = full_text.split("ประวัติการทำงาน/ฝึกงาน")[1].split("ความสามารถ")[0]
            else: history_text = ""
            thai_months_str = "มกราคม|กุมภาพันธ์|มีนาคม|เมษายน|พฤษภาคม|มิถุนายน|กรกฎาคม|สิงหาคม|กันยายน|ตุลาคม|พฤศจิกายน|ธันวาคม"
            raw_chunks = re.split(f"({thai_months_str})\\s+\\d{{4}}\\s+-\\s+", history_text)
            jobs = []
            if len(raw_chunks) > 1:
                for k in range(1, len(raw_chunks), 2):
                    if k+1 < len(raw_chunks): jobs.append(raw_chunks[k] + raw_chunks[k+1]) 
            i = 0
            while True:
                check_xpath = f'//*[@id="mainTableTwoColumn"]/tbody/tr/td[2]/table/tbody/tr[2]/td[2]/table[{i+1}]'
                try:
                    if len(self.driver.find_elements(By.XPATH, check_xpath)) == 0: break
                except: break
                suffix = f"_{i+1}"
                company = get_val(f'{check_xpath}/tbody/tr[3]/td/div/span', True)
                if not company: company = get_val(f'{check_xpath}/tbody/tr[3]/td', True)
                if i < len(jobs):
                    block = jobs[i]
                    if not company:
                        comp_match = re.search(r"^.*(บริษัท|Ltd|Inc|Group|Organization|หจก|Limited).*$", block, re.MULTILINE | re.IGNORECASE)
                        company = comp_match.group(0).strip() if comp_match else ""
                data[f'ชื่อบริษัทที่เคยทำงาน{suffix}'] = company
                if company: all_work_history.append(company.strip())
                i += 1
        except: pass
        
        competitor_str = ", ".join(all_work_history)
        data['เคยทำบริษัทคู่แข่ง'] = competitor_str

        today_date = datetime.date.today()
        update_date = self.parse_thai_date_exact(raw_update_date)
        days_diff = 999
        if update_date: days_diff = (today_date - update_date).days

        full_name = f"{data.get('ชื่อ', '')} {data.get('นามสกุล', '')}"
        person_data = {
            "keyword": keyword, 
            "company": competitor_str,
            "degree": highest_degree_text,
            "salary_min": salary_min_txt,
            "salary_max": salary_max_txt,
            "id": data.get('รหัสใบสมัคร', '').strip(),
            "name": full_name,
            "age": data.get('อายุ', '-'),
            "positions": combined_positions, 
            "last_update": data['อัพเดทล่าสุด'],
            "link": url,
            "image_path": data.get('รูปภาพ', '')
        }

        printer.print(f"   🔥 เจอ: {highest_degree_text} | มหาลัย: {matched_uni} | {days_diff} วันก่อน", style="bold green")
        return data, days_diff, person_data

    # --- EMAIL & SHEETS (เหมือนเดิม) ---
    def send_single_email(self, subject_prefix, people_list, col_header="เคยทำงานบริษัท"):
        sender = os.getenv("EMAIL_SENDER")
        password = os.getenv("EMAIL_PASSWORD")
        receiver_list = []
        if MANUAL_EMAIL_RECEIVERS and len(MANUAL_EMAIL_RECEIVERS) > 0: receiver_list = MANUAL_EMAIL_RECEIVERS
        else:
             rec_env = os.getenv("EMAIL_RECEIVER")
             if rec_env: receiver_list = [rec_env]
        
        if not sender or not password or not receiver_list: return

        if "สรุป" in subject_prefix or "HOT" in subject_prefix: subject = subject_prefix
        elif len(people_list) > 1: subject = f"🔥 {subject_prefix} ({len(people_list)} คน)"
        else: subject = subject_prefix 

        body_html = f"""
        <html>
        <head>
        <style>
            table {{ border-collapse: collapse; width: 100%; font-size: 14px; }}
            th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .btn {{
                background-color: #28a745; 
                color: #ffffff !important; 
                padding: 5px 10px;
                text-align: center; 
                text-decoration: none; 
                display: inline-block;
                border-radius: 4px; 
                font-size: 12px;
                font-weight: bold;
            }}
            .btn:hover {{ color: #ffffff !important; }}
        </style>
        </head>
        <body>
            <h3>{subject}</h3>
            <table>
                <tr>
                    <th style="width: 10%;">รูปภาพ</th>
                    <th style="width: 15%;">{col_header}</th>
                    <th style="width: 10%;">ระดับการศึกษาสูงสุด</th>
                    <th style="width: 10%;">รหัสใบสมัคร</th>
                    <th style="width: 15%;">ชื่อ-นามสกุล</th>
                    <th style="width: 5%;">อายุ</th>
                    <th style="width: 15%;">ตำแหน่งที่สมัคร</th>
                    <th style="width: 8%;">เงินเดือนขั้นต่ำ</th> <th style="width: 8%;">เงินเดือนสูงสุด</th> <th style="width: 10%;">อัพเดทล่าสุด</th>
                    <th style="width: 10%;">ลิงก์</th>
                </tr>
        """
        
        images_to_attach = []
        for person in people_list:
            cid_id = f"img_{person['id']}"
            if person['image_path'] and os.path.exists(person['image_path']):
                img_html = f'<img src="cid:{cid_id}" width="80" style="border-radius: 5px;">'
                images_to_attach.append({'cid': cid_id, 'path': person['image_path']})
            else:
                img_html = '<span style="color:gray;">No Image</span>'

            company_display = person['company']
            company_style = "font-weight: bold;" if (company_display == "University Target" or company_display == "-") else "font-weight: normal;"
            if company_display == "University Target" or company_display == "-": company_display = "-"

            body_html += f"""
                <tr>
                    <td style="text-align: center;">{img_html}</td>
                    <td style="{company_style}">{company_display}</td>
                    <td>{person.get('degree', '-')}</td> 
                    <td>{person['id']}</td>
                    <td>{person['name']}</td>
                    <td>{person['age']}</td>
                    <td>{person['positions']}</td>
                    <td>{person.get('salary_min', '-')}</td> <td>{person.get('salary_max', '-')}</td> <td>{person['last_update']}</td>
                    <td style="text-align: center;">
                        <a href="{person['link']}" target="_blank" class="btn">เปิดดู</a>
                    </td>
                </tr>
            """
            
        body_html += "</table><br><p><i>ระบบอัตโนมัติ JobThai Scraper</i></p></body></html>"

        try:
            server = smtplib.SMTP('smtp.gmail.com', 587)
            server.starttls()
            server.login(sender, password)
            
            msg_root = MIMEMultipart('related')
            msg_root['From'] = sender
            msg_root['Subject'] = subject
            
            msg_alternative = MIMEMultipart('alternative')
            msg_root.attach(msg_alternative)
            msg_alternative.attach(MIMEText(body_html, 'html'))
            
            for img_data in images_to_attach:
                try:
                    with open(img_data['path'], 'rb') as f:
                        msg_img = MIMEImage(f.read())
                        msg_img.add_header('Content-ID', f"<{img_data['cid']}>")
                        msg_img.add_header('Content-Disposition', 'inline', filename=os.path.basename(img_data['path']))
                        msg_root.attach(msg_img)
                except: pass

            for rec in receiver_list:
                if 'To' in msg_root: del msg_root['To']
                msg_root['To'] = rec
                server.send_message(msg_root)
                console.print(f"   ✅ ส่งเมล '{subject}' -> {rec}", style="success")
            server.quit()
        except Exception as e:
            console.print(f"❌ ส่งอีเมลล้มเหลว: {e}", style="error")

    def send_batch_email(self, batch_candidates, keyword):
        self.send_single_email(f"สรุปผู้สมัครรายสัปดาห์: {keyword} ({len(batch_candidates)} คน)", batch_candidates)

    def save_to_google_sheets(self):
        if not self.all_scraped_data:
            console.print("⚠️ ไม่มีข้อมูลใหม่ให้บันทึก", style="yellow")
            return

        console.rule("[bold green]📊 อัพโหลดขึ้น Google Sheets[/]")
        try:
            if not G_SHEET_KEY_JSON or not G_SHEET_NAME:
                console.print("❌ ไม่พบ Key หรือชื่อไฟล์ Google Sheet", style="error")
                return

            creds_dict = json.loads(G_SHEET_KEY_JSON)
            scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
            creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
            client = gspread.authorize(creds)
            
            sheet = client.open(G_SHEET_NAME)
            today_str = datetime.datetime.now().strftime("%d-%m-%Y")
            try:
                worksheet = sheet.worksheet(today_str)
            except:
                worksheet = sheet.add_worksheet(title=today_str, rows="100", cols="20")
                headers = [
                    "Link", "Keyword", "รหัสใบสมัคร", "ชื่อ-นามสกุล", "อายุ", "เพศ", 
                    "เบอร์โทร", "Email", "ที่อยู่", "ระดับการศึกษา", "มหาลัย", "คณะ", "สาขา",
                    "ตำแหน่งที่สมัคร", "เงินเดือนที่ขอ (Raw)", "เงินเดือนต่ำสุด", "เงินเดือนสูงสุด",
                    "เคยทำบริษัทคู่แข่ง", "อัพเดทล่าสุด"
                ]
                worksheet.append_row(headers)

            data_rows = []
            for item in self.all_scraped_data:
                row = [
                    item.get('Link', ''), item.get('Keyword', ''), item.get('รหัสใบสมัคร', ''),
                    f"{item.get('ชื่อ','')} {item.get('นามสกุล','')}", item.get('อายุ', ''), item.get('เพศ', ''),
                    re.sub(r'\D', '', str(item.get('เบอร์โทร', ''))), str(item.get('Email', '')).replace('Click', '').strip(),
                    item.get('จังหวัดที่อยู่', ''), item.get('ระดับการศึกษา', ''), item.get('มหาลัย', ''),
                    item.get('คณะ', ''), item.get('สาขา', ''),
                    f"{item.get('ตำแหน่งที่ต้องการสมัคร_1','')} {item.get('ตำแหน่งที่ต้องการสมัคร_2','')}",
                    item.get('เงินเดือนที่ต้องการ', ''), item.get('Salary_Min', '-'), item.get('Salary_Max', '-'), 
                    item.get('เคยทำบริษัทคู่แข่ง', ''), item.get('อัพเดทล่าสุด', '')
                ]
                data_rows.append(row)
            
            if data_rows:
                worksheet.append_rows(data_rows)
                console.print(f"✅ บันทึกข้อมูล {len(data_rows)} แถว เรียบร้อย!", style="bold green")
                
        except Exception as e:
            console.print(f"❌ Google Sheets Error: {e}", style="error")

    def run(self):
        self.email_report_list = []
        if not self.step1_login(): return
        
        today = datetime.date.today()
        is_monday = (today.weekday() == 0)
        is_manual_run = (os.getenv("GITHUB_EVENT_NAME") == "workflow_dispatch")
        
        console.print(f"📅 Status Check: Monday? [{'Yes' if is_monday else 'No'}] | Manual? [{'Yes' if is_manual_run else 'No'}]", style="bold yellow")
        
        for index, keyword in enumerate(SEARCH_KEYWORDS):
            console.rule(f"[bold magenta]🔍 คำค้นที่ {index+1}/{len(SEARCH_KEYWORDS)}: {keyword}[/]")
            
            current_keyword_batch = []
            if self.step2_search(keyword):
                links = self.step3_collect_all_links()
                if links:
                    console.print(f"\n🚀 เริ่มดูดข้อมูล '{keyword}' ({len(links)} รายการ) ...")
                    with Progress(
                        SpinnerColumn(), TextColumn("[progress.description]{task.description}"),
                        BarColumn(), TaskProgressColumn(), TimeElapsedColumn(), TimeRemainingColumn(),
                        console=console
                    ) as progress:
                        task_id = progress.add_task(f"[cyan]Processing...", total=len(links))
                        
                        for i, link in enumerate(links):
                            if self.total_profiles_viewed > 0 and self.total_profiles_viewed % 33 == 0:
                                progress.console.print(f"[yellow]☕ ครบ {self.total_profiles_viewed} คน... พัก 4 นาที[/]")
                                time.sleep(240)

                            try:
                                d, days_diff, person_data = self.scrape_detail_from_json(link, keyword, progress_console=progress.console)
                                self.total_profiles_viewed += 1 
                                
                                if d is not None:
                                    d['Keyword'] = keyword
                                    self.all_scraped_data.append(d)
                                    
                                    # Logic ส่งเมล (Hot / Weekly)
                                    should_add = False
                                    if days_diff <= 30:
                                        should_add = True
                                        if EMAIL_USE_HISTORY and person_data['id'] in self.history_data:
                                            try:
                                                last_notify = datetime.datetime.strptime(self.history_data[person_data['id']], "%Y-%m-%d").date()
                                                if (today - last_notify).days < 7: should_add = False
                                            except: pass
                                        if should_add: current_keyword_batch.append(person_data)

                                    if days_diff <= 1:
                                        should_hot = True
                                        if EMAIL_USE_HISTORY and person_data['id'] in self.history_data:
                                            try:
                                                 last_notify = datetime.datetime.strptime(self.history_data[person_data['id']], "%Y-%m-%d").date()
                                                 if (today - last_notify).days < 1: should_hot = False
                                            except: pass
                                        if should_hot:
                                            hot_subject = f"🔥 [HOT] พบผู้สมัครด่วน ({keyword}): {person_data['name']}"
                                            progress.console.print(f"   🚨 พบผู้สมัคร HOT -> ส่งเมลทันที!", style="bold red")
                                            self.send_single_email(hot_subject, [person_data], col_header="ประวัติบริษัท")
                                            if EMAIL_USE_HISTORY: self.history_data[person_data['id']] = str(today)

                                    if days_diff > 30 and (is_monday or is_manual_run):
                                        if current_keyword_batch:
                                             progress.console.print(f"\n[bold green]📨 ส่งเมลสรุป ({len(current_keyword_batch)} คน)![/]")
                                             self.send_batch_email(current_keyword_batch, keyword)
                                             if EMAIL_USE_HISTORY:
                                                 for p in current_keyword_batch: self.history_data[p['id']] = str(today)
                                             current_keyword_batch = []

                            except Exception as e: progress.console.print(f"[bold red]❌ Error Link {i+1}: {e}[/]")
                            progress.advance(task_id)
                
                if current_keyword_batch and (is_monday or is_manual_run):
                    self.send_batch_email(current_keyword_batch, keyword)
                    if EMAIL_USE_HISTORY:
                         for p in current_keyword_batch: self.history_data[p['id']] = str(today)

            console.print("⏳ พัก 3 วินาที...", style="dim")
            time.sleep(3)
        
        self.save_to_google_sheets()
        self.save_history()
        console.rule("[bold green]🏁 จบการทำงาน[/]")
        try: self.driver.quit()
        except: pass

if __name__ == "__main__":
    console.print("[bold green]🚀 Starting JobThai Scraper (Robust Edition)...[/]")
    if not MY_USERNAME or not MY_PASSWORD:
        console.print(f"\n[bold red]❌ [CRITICAL ERROR] ไม่พบ User/Pass ในไฟล์ .env[/]")
        exit()
    scraper = JobThaiRowScraper()
    scraper.run()
