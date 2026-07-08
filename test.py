from rich.progress import Progress
import time

with Progress() as p:
    t = p.add_task("Processing...", total=100)
    while not p.finished:
        p.update(t, advance=1)
        time.sleep(0.05)