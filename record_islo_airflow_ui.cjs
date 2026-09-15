const { chromium } = require("playwright");
const fs = require("node:fs");

const baseUrl = "http://localhost:28080";
const dagId = "common_ai_sandbox_toolset_islo_ui_demo";
const frameDir = "files/islo-airflow-ui-frames";

(async () => {
  fs.rmSync(frameDir, { recursive: true, force: true });
  fs.mkdirSync(frameDir, { recursive: true });
  const browser = await chromium.launch({
    headless: true,
    executablePath: "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
  });
  const context = await browser.newContext({ viewport: { width: 1440, height: 900 } });
  try {
    const page = await context.newPage();
    let frame = 0;
    const snap = async (holdFrames = 4) => {
      const firstPath = `${frameDir}/${String(frame).padStart(5, "0")}.png`;
      await page.screenshot({ path: firstPath, animations: "disabled", timeout: 10_000 });
      frame += 1;
      for (let duplicate = 1; duplicate < holdFrames; duplicate += 1) {
        fs.copyFileSync(firstPath, `${frameDir}/${String(frame).padStart(5, "0")}.png`);
        frame += 1;
      }
    };
    await page.goto(`${baseUrl}/`, { waitUntil: "networkidle" });
    if (await page.locator('input[name="username"]').count()) {
      await page.locator('input[name="username"]').fill("admin");
      await page.locator('input[name="password"]').fill("admin");
      await page.locator('button[type="submit"]').click();
      await page.waitForLoadState("networkidle");
    }
    const dagsLink = page.getByRole("link", { name: "Dags", exact: true });
    await dagsLink.click({ force: true });
    await page.waitForLoadState("networkidle");
    await page.waitForTimeout(1200);
    await snap(8);
    await page.getByRole("link", { name: dagId, exact: true }).click();
    await page.waitForLoadState("networkidle");

    const trigger = page.locator("button").filter({ hasText: /^Trigger$/ });
    for (let attempt = 0; attempt < 3 && !(await trigger.count()); attempt += 1) {
      await page.reload({ waitUntil: "networkidle" });
      await page.waitForTimeout(1000);
    }
    await page.waitForTimeout(1200);
    await snap(8);
    await trigger.click({ force: true });
    const dialog = page.getByRole("dialog");
    await dialog.waitFor();
    await page.waitForTimeout(1200);
    await snap(8);
    await dialog.getByRole("button", { name: /Trigger/ }).click();
    await page.waitForURL(new RegExp(`/dags/${dagId}/runs/`), { timeout: 30_000 });
    await page.waitForTimeout(1500);
    await snap(6);

    const taskLink = page.locator('a[href*="/runs/"][href$="/tasks/run_sandbox_agent"]').first();
    await taskLink.waitFor({ timeout: 30_000 });
    await taskLink.click();
    await page.waitForTimeout(1000);
    const logsLink = page.getByRole("link", { name: "Logs", exact: true });
    if (await logsLink.count()) {
      await logsLink.click();
    }
    for (let attempt = 0; attempt < 60; attempt += 1) {
      await page.waitForTimeout(1000);
      await snap(3);
      if ((await page.locator("body").innerText()).includes("ISLO SUCCESS")) {
        break;
      }
    }
    await snap(12);
    console.log(page.url());
    console.log((await page.locator("body").innerText()).slice(-4000));
  } finally {
    await context.close();
    await browser.close();
  }
})();
