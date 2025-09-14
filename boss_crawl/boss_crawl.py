#!/usr/bin/env python3
"""
Boss直聘岗位详情爬虫
从给定的Boss直聘岗位链接中提取信息并保存为JSON文件
"""

import asyncio
import json
import logging
import os
import re
import yaml
import time
from datetime import datetime
from typing import List, Dict, Any, Optional
from pathlib import Path

from playwright.async_api import async_playwright, Browser, Page, BrowserContext

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("boss_job_detail_crawler")


class BossJobDetailCrawler:
    """Boss直聘岗位详情爬虫"""

    def __init__(self, headless: bool = False):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.playwright = None

    async def start(self):
        """启动浏览器"""
        logger.info("启动浏览器...")
        self.playwright = await async_playwright().start()

        # 使用系统已安装的 Chrome
        user_data_dir = os.path.expanduser('~/boss_crawler_data')
        os.makedirs(user_data_dir, exist_ok=True)

        # 使用系统 Chrome
        self.browser = await self.playwright.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=self.headless,
            channel="chrome",  # 指定使用系统 Chrome
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-web-security',
                '--start-maximized'
            ],
            viewport={'width': 1280, 'height': 800},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        pages = self.browser.pages
        self.page = pages[0] if pages else await self.browser.new_page()
        logger.info("浏览器启动成功")

    async def ensure_logged_in(self):
        """确保已登录Boss直聘"""
        logger.info("检查登录状态...")

        # 导航到Boss直聘首页
        await self.page.goto("https://www.zhipin.com", wait_until="domcontentloaded")
        await asyncio.sleep(3)

        # 检查登录状态
        logged_in = False
        login_indicators = [
            'a[href*="/web/geek/chat"]',  # 聊天入口
            '.nav-figure img',  # 用户头像
            'a[ka="header-username"]',  # 用户名链接
        ]

        for indicator in login_indicators:
            try:
                element = await self.page.query_selector(indicator)
                if element:
                    logger.info("检测到已登录状态")
                    logged_in = True
                    break
            except:
                continue

        if not logged_in:
            logger.info("未检测到登录状态，请手动登录...")
            logger.info("请在浏览器中完成登录，然后按Enter继续")
            input("按Enter继续...")

            # 等待用户登录
            for _ in range(60):  # 最多等待5分钟
                await asyncio.sleep(5)
                for indicator in login_indicators:
                    try:
                        element = await self.page.query_selector(indicator)
                        if element:
                            logger.info("登录成功!")
                            return True
                    except:
                        continue
                logger.info("等待登录中...")

            logger.error("登录超时")
            return False

        return True

    async def extract_job_details(self, job_url: str) -> Dict[str, Any]:
        """从岗位链接提取详细信息"""
        logger.info(f"开始提取岗位详情: {job_url}")

        try:
            # 导航到岗位详情页
            await self.page.goto(job_url, wait_until="domcontentloaded")
            await asyncio.sleep(3)  # 等待页面加载

            # 等待关键元素出现
            try:
                await self.page.wait_for_selector('.job-banner', timeout=10000)
            except:
                logger.warning("页面加载可能超时，继续尝试提取")

            # 提取岗位信息
            job_details = {
                'url': job_url,
                'title': await self._extract_text('.name h1'),
                'salary': await self._extract_text('.salary'),
                'city': await self._extract_text('.text-city'),
                'experience': await self._extract_text('.text-experience'),
                'education': await self._extract_text('.text-degree'),
                'company': await self._extract_text('.company-info .name'),
                'company_type': await self._extract_text('.company-info .type'),
                'company_size': await self._extract_text('.company-info .size'),
                'job_description': await self._extract_text('.job-sec-text'),
                'extracted_at': datetime.now().isoformat()
            }

            # 提取标签信息
            tags = await self._extract_tags()
            if tags:
                job_details['tags'] = tags

            logger.info(f"成功提取岗位: {job_details.get('title', '未知')}")
            return job_details

        except Exception as e:
            logger.error(f"提取岗位详情失败: {e}")
            # 保存截图以便调试
            screenshot_path = f"error_screenshot_{int(time.time())}.png"
            await self.page.screenshot(path=screenshot_path)
            logger.info(f"已保存错误截图: {screenshot_path}")

            return {
                'url': job_url,
                'error': str(e),
                'extracted_at': datetime.now().isoformat()
            }

    async def _extract_text(self, selector: str) -> str:
        """提取文本内容"""
        try:
            element = await self.page.query_selector(selector)
            if element:
                text = await element.text_content()
                return text.strip() if text else ""
        except:
            pass
        return ""

    async def _extract_tags(self) -> List[str]:
        """提取标签信息"""
        tags = []
        try:
            tag_elements = await self.page.query_selector_all('.job-tags span')
            for tag_element in tag_elements:
                tag_text = await tag_element.text_content()
                if tag_text and tag_text.strip():
                    tags.append(tag_text.strip())
        except:
            pass
        return tags

    async def close(self):
        """关闭浏览器"""
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()
        logger.info("浏览器已关闭")


async def main():
    """主函数"""
    # 读取配置文件
    config_path = "job_links.yaml"
    if not os.path.exists(config_path):
        logger.error(f"配置文件不存在: {config_path}")
        print("请创建 job_links.yaml 文件并添加岗位链接")
        return

    with open(config_path, 'r', encoding='utf-8') as f:
        config = yaml.safe_load(f)

    job_links = config.get('job_links', [])
    if not job_links:
        logger.error("配置文件中没有找到岗位链接")
        return

    output_config = config.get('output', {})
    output_dir = output_config.get('dir', './job_details')
    output_format = output_config.get('format', 'json').lower()

    # 创建输出目录
    os.makedirs(output_dir, exist_ok=True)

    # 初始化爬虫
    crawler = BossJobDetailCrawler(headless=False)
    await crawler.start()

    # 确保登录
    if not await crawler.ensure_logged_in():
        await crawler.close()
        return

    # 提取所有岗位详情
    all_job_details = []

    print(f"\n开始提取 {len(job_links)} 个岗位详情...")
    print("=" * 50)

    for i, job_url in enumerate(job_links, 1):
        print(f"\n处理第 {i}/{len(job_links)} 个岗位:")
        print(f"URL: {job_url}")

        job_details = await crawler.extract_job_details(job_url)
        all_job_details.append(job_details)

        # 显示进度
        if 'title' in job_details:
            print(f"标题: {job_details['title']}")
        if 'error' in job_details:
            print(f"错误: {job_details['error']}")

        # 添加延迟避免请求过于频繁
        await asyncio.sleep(2)

    # 保存结果
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if output_format == 'json':
        output_path = os.path.join(output_dir, f'job_details_{timestamp}.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_job_details, f, ensure_ascii=False, indent=2)
        print(f"\n结果已保存为JSON: {output_path}")

    elif output_format == 'csv':
        # 简化的CSV导出
        import csv
        output_path = os.path.join(output_dir, f'job_details_{timestamp}.csv')

        # 获取所有可能的字段
        fieldnames = set()
        for job in all_job_details:
            fieldnames.update(job.keys())
        fieldnames = sorted(fieldnames)

        # 使用 utf-8-sig 编码而不是 utf-8
        with open(output_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for job in all_job_details:
                writer.writerow(job)

        print(f"\n结果已保存为CSV: {output_path}")

    # 显示统计信息
    success_count = sum(1 for job in all_job_details if 'error' not in job)
    error_count = len(all_job_details) - success_count

    print(f"\n提取完成!")
    print(f"成功: {success_count}, 失败: {error_count}")

    # 关闭浏览器
    await crawler.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n用户中断程序")
    except Exception as e:
        logger.error(f"程序执行出错: {e}")