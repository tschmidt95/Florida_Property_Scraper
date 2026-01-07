from pathlib import Path
p = Path('web_app.py')
s = p.read_text()
changed = False
# Try to find an input with name="query" and insert demo checkbox after that tag
idx = s.find('name="query"')
if idx != -1:
    # find end of the tag (first occurrence of '/>' after idx)
    end = s.find('/>', idx)
    if end != -1:
        insert_at = end + 2
        block = "\n    </div>\n  <div style=\"margin-top:8px\">\n    <label><input type=\"checkbox\" name=\"demo\" {% if demo %}checked{% endif %}/> Demo mode (no network)</label>\n  </div>"
        if 'name="demo"' not in s:
            s = s[:insert_at] + block + s[insert_at:]
            changed = True
# fallback: try to insert before closing </form>
if not changed:
    idx2 = s.find('</form>')
    if idx2 != -1:
        block = "\n  <div style=\"margin-top:8px\">\n    <label><input type=\"checkbox\" name=\"demo\" {% if demo %}checked{% endif %}/> Demo mode (no network)</label>\n  </div>\n"
        if 'name="demo"' not in s:
            s = s[:idx2] + block + s[idx2:]
            changed = True
if changed:
    p.write_text(s)
    print('patched web_app.py (added demo checkbox)')
else:
    print('pattern not found or demo already present')
