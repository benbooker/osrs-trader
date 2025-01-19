
import subprocess
import pywinctl as pwc
from time import sleep
from tqdm import tqdm
import pyautogui as pag
import random

rlLaunchTime = 8

def natural_click(min_x, max_x, min_y, max_y):
    pag.click(random.randint(min_x, max_x), random.randint(min_y, max_y))

def natural_write(text):
    for char in text:
        pag.write(char)
        sleep(random.uniform(0.05, 1.5))

def initialise(username, password):

    # if RuneLite application is not running, launch it quietly
    if len(pwc.getWindowsWithTitle("RuneLite")) == 0:
        print("Opening Runelite...")
        subprocess.Popen(
            ["runelite"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        # wait for RuneLite GUI to load
        for i in tqdm(range(100)):
            sleep(rlLaunchTime/100)
    else:
        print("RuneLite is already running.")
    
    # position and resize RuneLite window
    runeliteInstances = pwc.getWindowsWithTitle('RuneLite')
    rl1 = runeliteInstances[0]
    rl1.resizeTo(750,500)
    rl1.moveTo(0,0)
    
    # Log into account
    print(f"Logging into account {username}")
    natural_click(395, 535, 300, 338)
    natural_write(username)
    pag.press('tab')
    natural_write(password)
    pag.press('enter')
        
if __name__ == "__main__":
    initialise("userrnaaamee", "passworrrddd")





