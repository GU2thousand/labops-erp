import json
from pathlib import Path
text=Path('output/playwright/browser.log').read_text()
result=json.loads(text.split('### Result\n',1)[1].split('\n### ',1)[0])
Path('output/playwright/browser-results.json').write_text(json.dumps(result,indent=2))
print(json.dumps(result,indent=2))
assert len(result)>=16 and all(x['passed'] for x in result),result
