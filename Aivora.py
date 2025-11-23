# -------------------------------------------------------------------------
# Aivora Desk Local Worker (to be run on your laptop)
# This script handles communication with the hosted Next.js dashboard,
# downloads the job file, executes the Selenium bot, and streams logs back.
# -------------------------------------------------------------------------
import argparse
import logging
import os
import sys
import time
import requests
import json
import base64
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, List, Dict, Optional, Tuple, Callable

# --- START: User's Original Bot Code (x_commenter_bot_fixed.py) ---
# NOTE: The entire class and all helper functions from the user's submission
# are included here to make this file fully runnable and self-contained.

import pandas as pd
import random
import tempfile
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException, WebDriverException
)

# NOTE: Since this is a self-contained file, we must simulate chromedriver_autoinstaller
# or rely on the user having it, but for simplicity, we rely on the path being correct.
# In a real environment, chromedriver_autoinstaller would be used.
try:
    import chromedriver_autoinstaller
except ImportError:
    class MockAutoInstaller:
        def install(self): return "chromedriver"
    chromedriver_autoinstaller = MockAutoInstaller()

class XCommentBot:
    """ Automates X (Twitter) commenting with adaptive spreadsheet parsing and a no-terminal login flow. """

    def __init__(self, delay: float = 2.0, profile_path: str | None = None, headless: bool = False):
        self.delay = delay
        self.profile_path = profile_path
        self.headless = headless
        self.driver = None
        self.wait = None
        self.main_window = None
        self.results: List[Dict] = []
        self.original_df: Optional[pd.DataFrame] = None
        self.sheet_path: Optional[str] = None
        self._status_col_name: Optional[str] = None
        self._source_desc: str = ""
        self.ui_callback: Optional[Callable] = None # This will be the API streaming function
        self.setup_logging()

    def setup_logging(self):
        # We redirect logs to the custom handler which calls ui_callback
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info(f"X Commenter Bot initialized.")

    def log_and_callback(self, message: str, level: str = "info"):
        """Log message and call UI callback (API streamer) if available"""
        if level == "info":
            self.logger.info(message)
        elif level == "warning":
            self.logger.warning(message)
        elif level == "error":
            self.logger.error(message)

        if self.ui_callback:
            try:
                self.ui_callback(message, level)
            except Exception as e:
                # Log the error but don't break the main bot
                self.logger.error(f"UI callback (API stream) failed: {e}")

    def setup_chrome_driver(self) -> webdriver.Chrome:
        self.log_and_callback("Setting up Chrome WebDriver...")
        try:
            driver_path = chromedriver_autoinstaller.install()
            self.log_and_callback(f"ChromeDriver installed/found at: {driver_path}")
        except Exception:
            self.log_and_callback("Could not auto-install driver, assuming path is correct.", "warning")
            driver_path = "chromedriver"

        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=1280,900")
        else:
            chrome_options.add_argument("--no-headless")
            chrome_options.add_argument("--window-size=1280,900")
        
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)

        if self.profile_path:
            # Use provided path for persistence
            self.log_and_callback(f"Using persistent profile directory: {self.profile_path}")
            # Ensure path exists for first run
            Path(self.profile_path).mkdir(parents=True, exist_ok=True)
            chrome_options.add_argument(f"--user-data-dir={self.profile_path}")
        else:
            # Use temporary directory for non-persistent sessions (like the first headless=False run)
            temp_dir = tempfile.mkdtemp()
            chrome_options.add_argument(f"--user-data-dir={temp_dir}")
            self.log_and_callback(f"Using temporary profile directory: {temp_dir}")
        
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        
        service = Service(driver_path)
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.wait = WebDriverWait(self.driver, 20)
        self.log_and_callback("Chrome WebDriver setup completed successfully")
        return self.driver

    def navigate_to_login(self):
        self.log_and_callback("Navigating to X login page...")
        self.driver.get("https://x.com/i/flow/login")
        self.main_window = self.driver.current_window_handle
        self.log_and_callback("X login page loaded")

    def wait_for_manual_login(self):
        return self.wait_for_manual_login_ui()

    def wait_for_manual_login_ui(self) -> bool:
        """ Floating panel with an "I'm logged in" button. Keeps the panel visible across redirects by re-injecting it if it disappears. """
        if self.headless:
            self.log_and_callback("Running in headless mode. Assuming user is already logged in via profile.", "warning")
            # In headless mode, we can't show the UI, so we just check for login immediately
            # Try to navigate home and confirm login based on URL/element
            self.driver.get("https://x.com/home")
            time.sleep(3)
            return self.confirm_login()

        try:
            self.log_and_callback("=" * 60)
            self.log_and_callback("LOGIN FLOW")
            self.log_and_callback("1) Log in on X in the opened Chrome window.")
            self.log_and_callback("2) Click the floating 'I'm logged in' button to continue.")
            self.log_and_callback("=" * 60)

            self._inject_overlay_panel()
            start = time.time()
            timeout_seconds = 15 * 60
            last_log = 0
            last_url = ""
            while True:
                try:
                    present = self.driver.execute_script("return !!document.getElementById('xbot-login-overlay');")
                except Exception:
                    present = True 
                
                try:
                    current_url = self.driver.current_url
                except Exception:
                    current_url = last_url
                
                if (not present) or (current_url != last_url):
                    try:
                        self._inject_overlay_panel()
                    except Exception:
                        pass
                last_url = current_url
                
                try:
                    flag = self.driver.execute_script("return window.localStorage.getItem('xbot_login_ok');")
                except Exception:
                    flag = None
                
                if flag == "1":
                    self.log_and_callback("Login confirmed via UI button.")
                    try:
                        self.driver.execute_script("window.localStorage.removeItem('xbot_login_ok');")
                    except Exception:
                        pass
                    try:
                        self.driver.execute_script("var el = document.getElementById('xbot-login-overlay'); if (el) { el.remove(); }")
                    except Exception:
                        pass
                    break

                if self.confirm_login():
                    self.log_and_callback("Login auto-confirmed via page indicators.")
                    try:
                        self.driver.execute_script("var el = document.getElementById('xbot-login-overlay'); if (el) { el.remove(); }")
                    except Exception:
                        pass
                    break

                if time.time() - start > timeout_seconds:
                    self.log_and_callback("Login wait timed out after 15 minutes.", "error")
                    raise TimeoutException("Login not confirmed within timeout.")

                if time.time() - last_log > 10:
                    self.log_and_callback("Waiting for login confirmation... (click the overlay button when ready)")
                    last_log = time.time()
                
                time.sleep(1.0)
            return True
        except Exception as e:
            self.log_and_callback(f"Error during UI login wait: {str(e)}", "error")
            return False

    def _inject_overlay_panel(self):
        try:
            self.driver.execute_script(
                """
                (function(){
                    if (document.getElementById('xbot-login-overlay')) { return; }
                    var wrap = document.createElement('div');
                    wrap.id = 'xbot-login-overlay';
                    wrap.style.position = 'fixed';
                    wrap.style.right = '20px';
                    wrap.style.bottom = '20px';
                    wrap.style.zIndex = '999999';
                    wrap.style.background = 'rgba(20,20,20,0.92)';
                    wrap.style.color = '#fff';
                    wrap.style.padding = '16px';
                    wrap.style.borderRadius = '16px';
                    wrap.style.boxShadow = '0 8px 24px rgba(0,0,0,0.35)';
                    wrap.style.maxWidth = '320px';
                    wrap.style.fontFamily = 'system-ui, -apple-system, Segoe UI, Roboto, sans-serif';
                    
                    var title = document.createElement('div');
                    title.textContent = 'X Comment Bot';
                    title.style.fontSize = '16px';
                    title.style.fontWeight = '600';
                    title.style.marginBottom = '8px';
                    
                    var msg = document.createElement('div');
                    msg.textContent = 'Log in to X in this window, then click the button below to continue.';
                    msg.style.fontSize = '13px';
                    msg.style.opacity = '0.9';
                    msg.style.marginBottom = '12px';
                    
                    var bar = document.createElement('div');
                    bar.style.display = 'flex';
                    bar.style.gap = '8px';
                    
                    var btn = document.createElement('button');
                    btn.textContent = "I'm logged in";
                    btn.style.flex = '1';
                    btn.style.padding = '10px 12px';
                    btn.style.borderRadius = '12px';
                    btn.style.border = 'none';
                    btn.style.cursor = 'pointer';
                    btn.style.fontWeight = '600';
                    btn.style.fontSize = '14px';
                    btn.style.background = '#8B5CF6'; // Purple accent
                    btn.style.color = 'white'; 
                    
                    btn.addEventListener('click', function(){
                        try { window.localStorage.setItem('xbot_login_ok', '1'); } catch(e){}
                        var el = document.getElementById('xbot-login-overlay');
                        if (el) { el.remove(); }
                    }, { once: true });
                    
                    var cancel = document.createElement('button');
                    cancel.textContent = 'Hide';
                    cancel.style.padding = '10px 12px';
                    cancel.style.borderRadius = '12px';
                    cancel.style.border = '1px solid rgba(255,255,255,0.25)';
                    cancel.style.cursor = 'pointer';
                    cancel.style.background = 'transparent';
                    cancel.style.color = '#fff';
                    
                    cancel.addEventListener('click', function(){
                        var el = document.getElementById('xbot-login-overlay');
                        if (el) { el.remove(); }
                    }, { once: true });
                    
                    bar.appendChild(btn);
                    bar.appendChild(cancel);
                    wrap.appendChild(title);
                    wrap.appendChild(msg);
                    wrap.appendChild(bar);
                    
                    document.documentElement.appendChild(wrap);
                })();
                """
            )
            self.log_and_callback("Injected login confirmation overlay into the page.")
        except Exception as e:
            self.log_and_callback(f"Could not inject overlay panel: {e}", "warning")

    def confirm_login(self) -> bool:
        try:
            login_indicators = [
                (By.CSS_SELECTOR, "[data-testid='SideNav_AccountSwitcher_Button']"),
                (By.CSS_SELECTOR, "[data-testid='AppTabBar_Profile_Link']"),
                (By.CSS_SELECTOR, "[aria-label='Profile']"),
                (By.CSS_SELECTOR, "[data-testid='primaryColumn']")
            ]
            for selector_type, selector in login_indicators:
                try:
                    element = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((selector_type, selector))
                    )
                    if element is not None:
                        self.log_and_callback(f"Login confirmed via element: {selector}")
                        return True
                except TimeoutException:
                    continue
            
            current_url = self.driver.current_url
            if ("/home" in current_url) or (("x.com" in current_url) and ("/login" not in current_url)):
                self.log_and_callback(f"Login confirmed via URL pattern: {current_url}")
                return True
            
            return False
        except Exception as e:
            self.log_and_callback(f"Error during login confirmation: {str(e)}", "error")
            return False

    @staticmethod
    def _normalize(col: str) -> str:
        """Normalize column names for comparison"""
        if col is None:
            return ""
        return (
            str(col)
            .strip()
            .lower()
            .replace(" ", "_")
            .replace("-", "_")
        )

    def _detect_column(self, norm_cols: List[str], raw_cols: List[str], want: str) -> Optional[str]:
        """Detect column by type with improved matching"""
        candidates = []
        for norm, raw in zip(norm_cols, raw_cols):
            if want == "url":
                if norm in ("url", "posturl", "tweet_url", "link", "post_link"):
                    candidates.append(raw)
                elif ("url" in norm) and (("post" in norm) or ("tweet" in norm) or (norm == "url")):
                    candidates.append(raw)
            elif want == "comment":
                if norm in ("generated_comment", "comment", "reply", "comment_text", "generatedcomment"):
                    candidates.append(raw)
                elif ("comment" in norm) or ("reply" in norm) or ("generated" in norm and "comment" in norm):
                    candidates.append(raw)

        if len(candidates) > 0:
            return candidates[0]
        return None

    def _resolve_sheet_path(self, sheet_path: str) -> Path:
        """Improved path resolution with better error handling - now using temporary file path"""
        return Path(sheet_path).resolve() # In this worker, sheet_path is already a temporary resolved path

    def load_spreadsheet(self, sheet_input: Any) -> pd.DataFrame:
        """Robust spreadsheet loader with improved error handling - adapted for BytesIO input"""
        self.log_and_callback(f"Loading spreadsheet from bytes...")
        
        df = None
        read_errors: List[str] = []
        
        # We assume sheet_input is raw bytes from the API download
        if isinstance(sheet_input, (bytes, bytearray)):
            try:
                bio = BytesIO(sheet_input)
                self._source_desc = "API_Download_Bytes"
                
                # Try Excel first
                for engine in ["openpyxl", None, "calamine"]:
                    try:
                        bio.seek(0)
                        if engine == "calamine":
                            try:
                                df = pd.read_excel(bio, engine="calamine")
                            except ImportError:
                                continue 
                        else:
                            df = pd.read_excel(bio, engine=engine)
                        self.log_and_callback(f"Successfully read Excel with {engine or 'auto'} engine")
                        break
                    except Exception as e:
                        read_errors.append(f"Excel {engine or 'auto'}: {repr(e)}")

                # If Excel failed, try CSV
                if df is None:
                    for encoding in ["utf-8", "latin1", "cp1252"]:
                        try:
                            bio.seek(0)
                            df = pd.read_csv(bio, encoding=encoding)
                            self.log_and_callback(f"Successfully read CSV with {encoding} encoding")
                            break
                        except Exception as e:
                            read_errors.append(f"CSV {encoding}: {repr(e)}")
                            
                if df is None:
                    error_msg = "Error reading input file after fallbacks: " + " | ".join(read_errors)
                    self.log_and_callback(error_msg, "error")
                    raise ValueError(error_msg)

            except Exception as e:
                read_errors.append(f"Bytes processing: {repr(e)}")
        
        else:
            self.log_and_callback("Input was not recognized as bytes from API.", "error")
            raise ValueError("Input data type error.")

        # Keep a copy for writing status back
        self.original_df = df.copy()

        # Clean up column names and drop empty unnamed columns
        df.columns = [str(col).strip() for col in df.columns] # Remove trailing spaces
        df = df.loc[:, [c for c in df.columns if not (str(c).startswith("Unnamed") and pd.isna(df[c]).all())]]
        
        raw_cols = list(df.columns)
        norm_cols = [self._normalize(c) for c in raw_cols]
        self.log_and_callback(f"Loaded columns: {raw_cols}")
        
        # Detect URL and comment columns
        url_col = self._detect_column(norm_cols, raw_cols, "url")
        comment_col = self._detect_column(norm_cols, raw_cols, "comment")

        if url_col is None or comment_col is None:
            missing = []
            if url_col is None: missing.append("URL-like column (e.g., postUrl/url/link)")
            if comment_col is None: missing.append("comment-like column (e.g., Generated comment / comment / reply)")
            error_msg = f"Could not detect required columns: {', '.join(missing)}"
            self.log_and_callback(error_msg, "error")
            raise ValueError(error_msg)

        self.log_and_callback(f"Detected URL column: {url_col}")
        self.log_and_callback(f"Detected comment column: {comment_col}")

        # Standardize column names
        df["URL"] = df[url_col].astype(str).str.strip()
        df["generated_comment"] = (
            df[comment_col]
            .astype(str)
            .str.replace("\n", " ")
            .str.replace("\r", " ")
            .str.strip()
        )

        # Detect or create status column
        status_candidates = {"commented_(y/n)", "commented", "done", "posted", "status"}
        status_col = None
        for norm, raw in zip(norm_cols, raw_cols):
            if norm in status_candidates:
                status_col = raw
                break
        
        if status_col is None:
            status_col = "Commented (Y/N)"
        
        if status_col not in self.original_df.columns:
            self.original_df[status_col] = ""
            self.log_and_callback("Created 'Commented (Y/N)' column (was missing).")

        self._status_col_name = status_col
        
        # Clean and filter data
        before = len(df)
        df = df.dropna(subset=["URL", "generated_comment"])
        df = df[
            (df["URL"].astype(str).str.len() > 0)
            & (df["generated_comment"].astype(str).str.len() > 0)
            & (df["URL"].str.lower() != "nan")
            & (df["generated_comment"].str.lower() != "nan")
        ]
        self.log_and_callback(f"After cleaning empty rows: {before} -> {len(df)}")
        
        # Filter out already commented rows
        if status_col in df.columns:
            already = (df[status_col].astype(str).str.upper().str.strip().isin(["Y", "YES", "TRUE", "1"]))
            self.log_and_callback(f"Rows already commented (Y/YES/TRUE/1): {already.sum()}")
            df = df[~already].copy()
        
        self.log_and_callback(f"Final count - {len(df)} rows will be processed")
        
        return df

    def update_excel_file(self, row_index: int, status: str):
        """Update Excel file in memory with status - the full file update happens at the end"""
        try:
            if self.original_df is None: return
            
            status_col = self._status_col_name if self._status_col_name else "Commented (Y/N)"
            if status_col not in self.original_df.columns:
                self.original_df[status_col] = ""

            # Check if row_index is valid before assignment
            if row_index in self.original_df.index:
                self.original_df.loc[row_index, status_col] = status
                self.log_and_callback(f"Internal update: Row {row_index} marked as '{status}'")
            else:
                self.log_and_callback(f"Internal update error: Row index {row_index} not found in original data.", "warning")

        except Exception as e:
            self.log_and_callback(f"Error updating file in memory: {str(e)}", "error")

    def process_posts(self, df: pd.DataFrame):
        """Process posts with improved progress reporting"""
        if len(df) == 0:
            self.log_and_callback("No posts to process - all rows already commented or no valid data found.")
            return

        self.log_and_callback(f"Starting to process {len(df)} uncommented posts...")

        for current_post_idx, (idx, row) in enumerate(df.iterrows()):
            url = row["URL"]
            comment = row["generated_comment"]
            
            # The original bot checks this again, but we keep the logic clean here
            # if (self._status_col_name in row) and (str(row[self._status_col_name]).strip().upper() in {"Y", "YES", "TRUE", "1"}):
            #     self.log_and_callback(f" Skipping row {idx} - already commented")
            #     continue 

            current_post = current_post_idx + 1
            self.log_and_callback(f"Processing post {current_post}/{len(df)} (Original Index: {idx})")
            self.log_and_callback(f" URL: {url}")

            # NOTE: We pass the actual row index (idx) to update the correct row later
            result = self.process_single_post(url, comment, current_post, idx)
            self.results.append(result)
            
            status = "Y" if result["status"] == "success" else "N"
            self.update_excel_file(idx, status)

            # Progress update for UI
            if self.ui_callback:
                self.ui_callback(f"Progress: {current_post}/{len(df)}", "progress") 

            if current_post < len(df):
                delay_time = self.delay + random.uniform(0.5, 1.5)
                self.log_and_callback(f"Waiting {delay_time:.1f} seconds before next post...")
                time.sleep(delay_time)

        self.log_and_callback("Finished processing all posts")

    def process_single_post(self, url: str, comment: str, post_number: int, original_index: int) -> Dict:
        """Process a single post with improved error handling"""
        result = {
            "post_number": post_number,
            "original_index": original_index,
            "url": url,
            "comment": comment[:50] + "..." if len(comment) > 50 else comment,
            "status": "failed",
            "message": "",
            "timestamp": datetime.now().isoformat()
        }
        max_retries = 3
        for attempt in range(max_retries):
            try:
                self.driver.execute_script("window.open('');")
                self.driver.switch_to.window(self.driver.window_handles[-1])
                self.driver.get(url)
                time.sleep(3)
                
                if self.post_comment(comment):
                    result["status"] = "success"
                    result["message"] = "Comment posted successfully"
                    self.log_and_callback(f"✓ Post {post_number}: Comment posted successfully")
                    break
                else:
                    result["message"] = f"Failed to post comment (attempt {attempt + 1})"

            except Exception as e:
                error_msg = f"Error on attempt {attempt + 1}: {str(e)}"
                result["message"] = error_msg
                self.log_and_callback(f"✗ Post {post_number}: {error_msg}", "error")

            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                self.log_and_callback(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            
            finally:
                try:
                    self.driver.close()
                    self.driver.switch_to.window(self.main_window)
                except Exception:
                    pass
        
        return result

    def post_comment(self, comment: str) -> bool:
        """Post comment with improved element detection"""
        try:
            comment = comment.replace("\n", " ").replace("\r", " ").strip()
            self.log_and_callback(f"Attempting to post comment: {comment[:50]}...")

            # Find reply button
            reply_button_selectors = [
                "[data-testid='reply']", "[aria-label*='Reply']", 
                "[data-testid='tweetButtonInline']", "button[aria-label*='Reply']"
            ]
            reply_button = None
            for selector in reply_button_selectors:
                try:
                    reply_button = WebDriverWait(self.driver, 15).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    self.log_and_callback(f"Found reply button with selector: {selector}")
                    break
                except TimeoutException:
                    continue

            if reply_button is None:
                self.log_and_callback("Could not find reply button", "error")
                return False

            # Scroll to and click reply button
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", reply_button)
            time.sleep(1)
            self.log_and_callback("Clicking reply button...")
            reply_button.click()
            time.sleep(2)

            # Find compose area
            compose_selectors = [
                "[data-testid='tweetTextarea_0']", "[contenteditable='true'][role='textbox']", 
                ".public-DraftEditor-content", "[aria-label*='Post your reply']", 
                "[placeholder*='Post your reply']", "div[contenteditable='true']"
            ]
            compose_area = None
            for selector in compose_selectors:
                try:
                    compose_area = WebDriverWait(self.driver, 10).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                    )
                    self.log_and_callback(f"Found compose area with selector: {selector}")
                    break
                except TimeoutException:
                    continue

            if compose_area is None:
                self.log_and_callback("Could not find compose text area", "error")
                return False

            # Input comment text with multiple fallback methods
            self.log_and_callback("Clicking on compose area...")
            compose_area.click()
            time.sleep(1)
            self.log_and_callback("Inputting comment text...")
            input_success = False

            # Method 1: Standard send_keys
            try:
                compose_area.clear()
                compose_area.send_keys(comment)
                input_success = True
                self.log_and_callback("Comment text entered successfully using send_keys")
            except Exception:
                 # Method 2: ActionChains (often better for contenteditable)
                try:
                    actions = ActionChains(self.driver)
                    actions.click(compose_area)
                    actions.key_down(Keys.CONTROL).send_keys("a").key_up(Keys.CONTROL).send_keys(Keys.DELETE)
                    actions.send_keys(comment)
                    actions.perform()
                    input_success = True
                    self.log_and_callback("Comment text entered using ActionChains")
                except Exception:
                    # Method 3: JavaScript
                    try:
                        self.driver.execute_script(
                            "arguments[0].innerText = arguments[1]; arguments[0].dispatchEvent(new Event('input', {bubbles: true}));",
                            compose_area, comment
                        )
                        input_success = True
                        self.log_and_callback("Comment text entered using JavaScript")
                    except Exception:
                        pass
            
            if not input_success:
                self.log_and_callback("Failed to input comment text with all methods", "error")
                return False

            # Find and click post button
            self.log_and_callback("Waiting for Post/Reply button to become enabled...")
            time.sleep(2)

            post_button_selectors = [
                "[data-testid='tweetButton']", "[data-testid='tweetButtonInline']", 
                "button[role='button']", "[aria-label*='Reply']", "[aria-label*='Post']"
            ]
            post_button = None
            
            for selector in post_button_selectors:
                try:
                    if selector == "button[role='button']":
                        xpath_selector = "//button[not(@disabled) and (contains(., 'Reply') or contains(., 'Post'))]"
                        post_button = WebDriverWait(self.driver, 8).until(
                            EC.element_to_be_clickable((By.XPATH, xpath_selector))
                        )
                    else:
                        post_button = WebDriverWait(self.driver, 8).until(
                            EC.element_to_be_clickable((By.CSS_SELECTOR, selector))
                        )

                    if post_button.is_enabled():
                        self.log_and_callback(f"Found enabled post button with selector: {selector}")
                        break
                    else:
                        self.log_and_callback(f"Post button found but disabled: {selector}")
                        post_button = None
                except TimeoutException:
                    continue

            if post_button is None:
                self.log_and_callback("Could not find enabled Post/Reply button", "error")
                return False

            # Click post button
            self.driver.execute_script("arguments[0].scrollIntoView({block:'center'});", post_button)
            time.sleep(1)
            self.log_and_callback("Clicking Post/Reply button...")
            post_button.click()

            # Wait and verify posting
            self.log_and_callback("Waiting for comment to be posted...")
            time.sleep(5)
            
            try:
                # Look for the tweet cell/container to confirm successful posting
                success_indicators = ["[data-testid='tweet']", "[data-testid='cellInnerDiv']"]
                for indicator in success_indicators:
                    try:
                        WebDriverWait(self.driver, 3).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, indicator))
                        )
                        self.log_and_callback(f"Comment posting verified via: {indicator}")
                        break
                    except TimeoutException:
                        continue
            except Exception as e:
                self.log_and_callback(f"Could not verify comment posting: {e}", "warning")

            self.log_and_callback("Comment posting process completed successfully")
            return True

        except Exception as e:
            self.log_and_callback(f"Error posting comment: {str(e)}", "error")
            return False

    def generate_summary_report(self) -> str:
        """Generate summary report"""
        total_posts = len(self.results)
        successful_posts = len([r for r in self.results if r["status"] == "success"])
        failed_posts = total_posts - successful_posts
        success_rate = (successful_posts / total_posts * 100) if total_posts > 0 else 0
        
        summary = f"""
X (Twitter) Commenter Bot - Session Summary
===========================================
Total posts processed: {total_posts}
Comments posted successfully: {successful_posts}
Failed attempts: {failed_posts}
Success rate: {success_rate:.1f}%

Session completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
Note: Posts already marked as 'Y' were automatically skipped.
"""
        if failed_posts > 0:
            summary += "\nFailed Posts:\n"
            for result in self.results:
                if result["status"] == "failed":
                    summary += f"- Post {result['post_number']} (Row {result['original_index']}): {result['message']}\n"
        return summary

    def get_updated_file_bytes(self) -> bytes:
        """Generates the updated Excel file (BytesIO) for upload back to the server."""
        if self.original_df is None:
            return b""
        
        # We enforce Excel format for consistency and to retain original formatting better
        output = BytesIO()
        self.original_df.to_excel(output, index=False, engine="openpyxl")
        output.seek(0)
        self.log_and_callback("Generated updated Excel file in memory.")
        return output.read()

    def cleanup(self):
        """Clean up resources"""
        if self.driver:
            try:
                self.driver.quit()
                self.log_and_callback("Browser closed successfully")
            except Exception as e:
                self.log_and_callback(f"Error closing browser: {str(e)}", "error")

    def run(self, sheet_data_bytes: bytes, delay: float, profile_path: str, headless: bool, on_update: Callable) -> Tuple[int, str]:
        """Main run method adapted for API communication"""
        self.delay = delay
        self.profile_path = profile_path
        self.headless = headless
        self.ui_callback = on_update

        try:
            self.setup_chrome_driver()
            
            # 1. Login
            self.navigate_to_login()
            if not self.wait_for_manual_login():
                self.log_and_callback("Login confirmation failed. Exiting...", "error")
                return 2, "Login failed."

            # 2. Load Data
            df = self.load_spreadsheet(sheet_data_bytes)
            if len(df) == 0:
                self.log_and_callback("No uncommented posts found to process.")
                return 4, "No posts to process."
            
            # 3. Process
            self.process_posts(df)
            
            # 4. Final Report
            summary = self.generate_summary_report()
            print(summary)
            self.log_and_callback(summary, "summary")

            successful = len([r for r in self.results if r["status"] == "success"])
            return 0 if successful > 0 else 3, summary

        except Exception as e:
            error_msg = f"Fatal error: {str(e)}"
            self.log_and_callback(error_msg, "error")
            return 1, error_msg
        finally:
            self.cleanup()
# --- END: User's Original Bot Code (x_commenter_bot_fixed.py) ---


# -------------------------------------------------------------------------
# Local Worker Communication Logic
# -------------------------------------------------------------------------

BASE_URL = "http://localhost:3000" # NOTE: Change this to your Vercel URL (e.g., https://aivora-desk.vercel.app)
JOB_ENDPOINT = f"{BASE_URL}/api/job"
POLLING_INTERVAL = 10 # seconds
MAX_POLLS = 1000 # Stop after 10,000 seconds (approx 2.7 hours)

# Global flag to track if this is the first run to trigger headless=False
PROFILE_DIR = Path.home() / ".aivora_x_profile"
IS_FIRST_RUN_FILE = PROFILE_DIR / ".first_run_completed"

def api_stream_log(job_id: str, message: str, level: str = "info"):
    """Sends log message back to the hosted Next.js API"""
    log_data = {
        "jobId": job_id,
        "message": message,
        "level": level,
        "timestamp": datetime.now().isoformat()
    }
    
    # We use a non-blocking request with a short timeout
    try:
        requests.post(
            f"{JOB_ENDPOINT}/log", 
            json=log_data, 
            timeout=2
        )
    except requests.exceptions.RequestException as e:
        # If API call fails, just print to console but don't stop the bot
        print(f"API Log Stream FAILED: {e}")

def get_job_file_bytes(file_id: str) -> Optional[bytes]:
    """Downloads the file from the hosted API"""
    print(f"Downloading file ID: {file_id}")
    try:
        response = requests.get(f"{JOB_ENDPOINT}/file/{file_id}")
        response.raise_for_status()
        
        # The file content is base64 encoded in the response body
        data = response.json()
        if data.get("base64Data"):
            return base64.b64decode(data["base64Data"])
        return None
    except requests.exceptions.RequestException as e:
        print(f"Error downloading file: {e}")
        return None

def update_job_status(job_id: str, status: str, summary: str, updated_file_bytes: Optional[bytes] = None):
    """Updates the job status and uploads the final file"""
    data = {
        "status": status,
        "summary": summary
    }
    
    if updated_file_bytes:
        data["updatedFileBase64"] = base64.b64encode(updated_file_bytes).decode('utf-8')
    
    print(f"Updating job {job_id} status to: {status}")
    try:
        requests.post(
            f"{JOB_ENDPOINT}/status/{job_id}", 
            json=data, 
            timeout=30 # longer timeout for file upload
        ).raise_for_status()
        print(f"Job {job_id} status updated successfully on server.")
    except requests.exceptions.RequestException as e:
        print(f"Error updating job status on server: {e}")

def check_for_pending_jobs():
    """Polls the API to check for pending jobs"""
    print(f"Polling {JOB_ENDPOINT}/pending for jobs...")
    try:
        response = requests.get(f"{JOB_ENDPOINT}/pending", timeout=10)
        response.raise_for_status()
        return response.json().get("job")
    except requests.exceptions.RequestException as e:
        print(f"Error polling for jobs: {e}")
        return None

def worker_main():
    """Main loop for the local worker"""
    
    # 1. Setup profile directory
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    is_first_run = not IS_FIRST_RUN_FILE.exists()
    
    if is_first_run:
        print("--- FIRST RUN DETECTED ---")
        print("The first run must be done with headless=False for manual login.")
        print("A profile will be created at:", PROFILE_DIR)
        print("Please log in to X in the pop-up window and click 'I'm logged in'.")
    else:
        print("--- SUBSEQUENT RUN ---")
        print("The bot will attempt to run headless using the saved profile.")

    poll_count = 0
    while poll_count < MAX_POLLS:
        job = check_for_pending_jobs()
        
        if job:
            job_id = job.get("id")
            file_id = job.get("fileId")
            delay = job.get("delay") or 5.0
            
            print(f"\n--- JOB {job_id} RECEIVED ---")
            
            # Set job status to RUNNING immediately
            update_job_status(job_id, "RUNNING", "Worker picked up job.")

            # Download file
            file_bytes = get_job_file_bytes(file_id)
            if not file_bytes:
                update_job_status(job_id, "FAILED", "Could not download file from server.")
                time.sleep(POLLING_INTERVAL)
                continue

            # Log streaming wrapper
            def job_log_streamer(message: str, level: str = "info"):
                api_stream_log(job_id, message, level)
            
            # Configure bot run
            headless_mode = not is_first_run
            
            # EXECUTE THE BOT
            bot = XCommentBot()
            exit_code, final_summary = bot.run(
                sheet_data_bytes=file_bytes,
                delay=delay,
                profile_path=str(PROFILE_DIR.resolve()), # Pass the persistent profile path
                headless=headless_mode,
                on_update=job_log_streamer
            )

            # Get the updated file bytes
            updated_file_bytes = bot.get_updated_file_bytes()
            
            # Update final job status
            if exit_code == 0:
                final_status = "COMPLETED"
            elif exit_code == 4:
                 final_status = "COMPLETED" # No posts to process is a success
            else:
                final_status = "FAILED"
            
            update_job_status(job_id, final_status, final_summary, updated_file_bytes)
            
            # Mark first run complete so next run is headless
            if is_first_run and final_status in ["COMPLETED", "FAILED"]:
                IS_FIRST_RUN_FILE.touch()
                print("First run marker created. Next run will be headless.")
            
            print(f"--- JOB {job_id} FINISHED. Status: {final_status} ---\n")
            poll_count = 0 # Reset poll count after a successful job

        else:
            poll_count += 1
            print(f"No pending jobs found. Polling again in {POLLING_INTERVAL}s. (Poll {poll_count})")
            time.sleep(POLLING_INTERVAL)

if __name__ == "__main__":
    worker_main()
