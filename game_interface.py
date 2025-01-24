
import subprocess
import pywinctl as pwc
from time import sleep
from tqdm import tqdm
import pyautogui as pag
import random

from constants import *

class ClickField():
    def __init__(self, min_x: int, max_x: int, min_y: int, max_y: int):
        self.min_x = min_x
        self.max_x = max_x
        self.min_y = min_y
        self.max_y = max_y

    def __repr__(self):
            return f"ClickField(min_x={self.min_x}, max_x={self.max_x}, min_y={self.min_y}, max_y={self.max_y})"
    
    def natural_click(self):
        """Performs a click at a random location within the defined field."""
        x = random.randint(self.min_x, self.max_x)
        y = random.randint(self.min_y, self.max_y)
        pag.click(x, y)


def natural_write(text):
    for char in text:
        pag.write(char)
        sleep(random.uniform(0.05, 1.5))


class RLInstance():
    def __init__(self, username: str, password: str, membership: bool):
        self.username = username
        self.password = password
        self.membership = membership

    def setup(self):
        # Close any RuneLite windows that might already be open
        runelite_instances = pwc.getWindowsWithTitle('RuneLite')
        if runelite_instances:
            print(f"Closing {len(runelite_instances)} existing RuneLite instances...")
            for instance in runelite_instances:
                instance.close()
                sleep(1)
                pag.press('enter')
                sleep(1)
        else:
            print("No existing RuneLite instances found.")

        # Open a single new RuneLite window
        print("Opening RuneLite...")
        subprocess.Popen(
            ["runelite"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

        # Wait for RuneLite GUI to load
        for i in tqdm(range(100)):
            sleep(RL_LAUNCH_TIME / 100)
        
        # Position and resize RuneLite window
        runelite_instances = pwc.getWindowsWithTitle('RuneLite')
        instance = runelite_instances[0]
        instance.raiseWindow()
        instance.resizeTo(RL_WIDTH, RL_HEIGHT)
        instance.moveTo(TOP_LEFT_PIXEL,TOP_LEFT_PIXEL)
        
        # Log into account
        print(f"Logging into account {self.username}")
        pag.press('enter')
        natural_write(self.username)
        pag.press('tab')
        natural_write(self.password)
        pag.press('enter')
            
        
if __name__ == "__main__":
    username = "yo"
    password = "yo"
    membership = False

    rl_instance = RLInstance(username, password, membership)
    rl_instance.setup()

