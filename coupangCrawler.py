import undetected_chromedriver as uc
import pymysql
import logging
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from contextlib import contextmanager
from selenium.webdriver.common.proxy import Proxy, ProxyType

class CoupangCrawler:
    def __init__(self):
        self.setup_logging()
        self.setup_database()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.ERROR,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler("crawler_debug.log"),
                logging.StreamHandler(),
            ],
        )
        self.logger = logging.getLogger(__name__)

    def setup_database(self):
        try:
            self.db_config = {
                "host": "",
                "port": "",
                "user": "",
                "password": "",                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  
                
                "database": "",
                "charset": "utf8mb4",
                "cursorclass": pymysql.cursors.DictCursor,
            }
            self.test_connection()
        except Exception as e:
            self.logger.error(f"데이터베이스 연결 실패: {str(e)}")
            raise

    @contextmanager
    def get_db_connection(self):
        conn = None
        try:
            conn = pymysql.connect(**self.db_config)
            yield conn
        finally:
            if conn:
                conn.close()

    def test_connection(self):
        with self.get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")

    def get_products_from_database(self):
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT DISTINCT PRODUCT_CODE, OPTION_CODE 
                        FROM CPS_PRODUCT 
                        WHERE PRODUCT_CODE IS NOT NULL AND OPTION_CODE IS NOT NULL
                    """)
                    results = cursor.fetchall()
                    return [{"product_code": row["PRODUCT_CODE"], "option_code": row["OPTION_CODE"]} for row in results]
        except Exception as e:
            self.logger.error(f"데이터 조회 실패: {str(e)}")
            return []      

    def update_product(self, product_code, option_code):
        try:
            with self.get_db_connection() as conn:
                with conn.cursor() as cursor:
                    sql = """
                        UPDATE CPS_PRODUCT 
                        SET MOD_DATE = NOW() 
                        WHERE PRODUCT_CODE = %s AND OPTION_CODE = %s
                    """
                    result = cursor.execute(sql, (product_code, option_code))
                    conn.commit()
                    return result > 0
        except Exception as e:
            self.logger.error(f"데이터 업데이트 실패: {str(e)}")
            return False

    def crawl_products(self, product_chunk):
        success = 0 
        failed = 0
        total = 0;
        for product in product_chunk:
            # try:
            #     import psutil
            #     # 크롬드라이버와 크롬 프로세스 모두 종료
            #     for proc in psutil.process_iter():
            #         try:
            #             if "chromedriver" in proc.name().lower() or "chrome.exe" in proc.name().lower():
            #                 proc.kill()
            #         except (psutil.NoSuchProcess, psutil.AccessDenied):
            #             pass
            # except Exception:
            #     pass

            driver = None
            try:
                options = uc.ChromeOptions()

                options.add_argument("--headless=new")
                options.add_argument("--no-sandbox")
                options.add_argument("--disable-dev-shm-usage")
                options.add_argument("--disable-gpu")
                options.add_argument("--window-size=1920,1080")

                debug_port = random.randint(9222, 9999)
                options.add_argument(f"--remote-debugging-port={debug_port}")
                options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
                
                driver = uc.Chrome(options=options)
                driver.set_page_load_timeout(30)
                url = f"https://www.coupang.com/vp/products/{product['product_code']}?itemId={product['option_code']}"
                driver.get(url)

                product_data = {
                    "url": url,
                    "product_code": product['product_code'],
                    "option_code": product['option_code'],
                    "name" : "",
                    "category" : "",
                    "rocket_status" : "",
                    "price" : "",
                    "rocket_price" : "",
                    "card_info" : {},
                    "data_none" : ""
                }

                # 상품명 확인
                try:
                    product_data["name"] = driver.find_element(By.CSS_SELECTOR, "h1.prod-buy-header__title").text
                except Exception:
                    pass

                # 카테고리 및 마우스 액션
                try:
                    category_element = driver.find_element(By.XPATH, "(//ul[@id='breadcrumb']/li)[3]")
                    product_data["category"] = category_element.find_element(By.TAG_NAME, "a").get_dom_attribute("title")
                    
                    # 마우스 움직임 시뮬레이션
                    actions = ActionChains(driver)
                    random_x = random.randint(100, 300)
                    random_y = random.randint(100, 300)
                    actions.move_to_element(category_element)\
                          .move_by_offset(random_x, random_y)\
                          .perform()
                except Exception:
                    pass

                # 로켓배송 상태 확인
                try:
                    badges = driver.find_elements(By.CLASS_NAME, "td-delivery-badge")
                    for badge in badges:
                        img = badge.find_element(By.TAG_NAME, "img")
                        src = img.get_dom_attribute("src")
                        if "rocket-fresh" in src:
                            product_data["rocket_status"] = "fresh"
                            break
                        elif "rocket_logo" in src:
                            product_data["rocket_status"] = "rocket"
                            break
                        elif "global_b" in src:
                            product_data["rocket_status"] = "global"
                            break
                except Exception:
                    pass
                
                # 가격 수집
                try:
                    price_element = driver.find_element(By.CSS_SELECTOR, ".prod-sale-price .total-price")
                    product_data["price"] = int("".join(filter(str.isdigit, price_element.text)))
                except Exception:
                    try:
                        price_element = driver.find_element(By.CSS_SELECTOR, ".prod-price .total-price")
                        product_data["price"] = int("".join(filter(str.isdigit, price_element.text)))
                    except Exception:
                        pass

                # 와우 회원 가격 수집
                try:
                    wow_price_element = driver.find_element(By.CSS_SELECTOR, ".prod-price .prod-coupon-price .total-price")
                    product_data["rocket_price"] = int("".join(filter(str.isdigit, wow_price_element.text)))
                except Exception:
                    pass

                # 카드 혜택 정보 수집
                card_benefits = []
                try:
                    benefit_badges = driver.find_elements(By.CSS_SELECTOR, ".ccid-benefit-badge")
                    for badge in benefit_badges:
                        try:
                            img = badge.find_element(By.CSS_SELECTOR, "img.benefit-ico")
                            src = img.get_dom_attribute("src")
                            if "@" in src:
                                card_info = {                                                                                                                                                                                                                             
                                    "merchant_id": "coupang",
                                    "product_code": product["product_code"],
                                    "option_code": product["option_code"],
                                    "card": src.split("web/")[1].split("@")[0],
                                    "member_exclusive": "Y" if badge.find_element(By.CSS_SELECTOR, ".benefit-label-highlight").text.strip() else "N",
                                    "discount": int(badge.find_element(By.CSS_SELECTOR, ".benefit-label b").text.replace("%", ""))
                                }
                                card_benefits.append(card_info)
                        except Exception:
                            continue
                except Exception:
                    pass
                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                try:
                    product_data["data_none"] = driver.find_element(By.CSS_SELECTOR, "div.prod-not-find-unknown a.prod-not-find-unknown__p").text
                except Exception:
                    pass

                # 메서드 호출 시 필요한 모든 파라미터 전달
                if product_data.get("name") or product_data.get("data_none") is False:
                    result = self.update_product(product_data["product_code"], product_data["option_code"])
                    if result > 0:
                        print("업데이트 성공")
                        success += 1
                        print(f"{len(product_chunk)}중 {success}개 성공")                                                                                                                                                                                                                                                                                                                              
                else:
                    print("업데이트 실패")

                # 크롤링 결과 출력
                print("\n" + "=" * 50)
                print("-" * 50)
                for key, value in product_data.items():
                    print(f"{key:<15}: {value}")
                print("=" * 50)
            except Exception as e:
                failed += 1
                self.logger.error(f"크롤링 실패 ({product['product_code']}): {str(e)}")
            finally:
                if driver:
                    driver.quit()
                time.sleep(random.uniform(1, 3))

if __name__ == "__main__":
    crawler = None
    try:
        crawler = CoupangCrawler()
        products = crawler.get_products_from_database()
        if products:
            print(f"\n총 {len(products)}개의 상품을 크롤링합니다...")
            crawler.crawl_products(products)
            print("\n크롤링이 완료되었습니다.")
        else:
            print("\n크롤링할 상품이 없습니다.")
    except KeyboardInterrupt:
        print("\n프로그램을 종료합니다.")
    except Exception as e:
        print(f"\n오류가 발생했습니다: {str(e)}")
