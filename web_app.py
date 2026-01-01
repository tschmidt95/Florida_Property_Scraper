import os
import subprocess
from flask import Flask, request, render_template_string

app = Flask(__name__)

HTML = """
<!doctype html>
<title>Florida Property Scraper (Internal)</title>
<h2>Florida Property Scraper (Internal)</h2>
<form method="post">
  <div>
    <input name="name" style="width:300px" placeholder="Owner name" value="{{name|default('')}}" />
    <input name="address" style="width:300px; margin-left:8px" placeholder="Address (paired with name)" value="{{address|default('')}}" />
  </div>
  <div style="margin-top:8px">or</div>
  <div style="margin-top:8px">
    <input name="query" style="width:620px" placeholder="Owner name or address (single-field query)" value="{{query|default('')}}" />
  </div>
  <button type="submit" style="margin-top:8px">Run</button>
</form>
<pre style="margin-top:16px; padding:12px; background:#f6f6f6; border:1px solid #ddd;">{{output|default('')}}</pre>
"""

@app.route("/", methods=["GET", "POST"])
def home():
    query = ""
    name = ""
    address = ""
    output = ""
    if request.method == "POST":
        name = request.form.get("name", "").strip()
        address = request.form.get("address", "").strip()
        query = request.form.get("query", "").strip()

        env = os.environ.copy()

        if name:
            if not address:
                output = "When providing a name you must also provide an address."
            else:
                cmd = ["python", "-m", "florida_property_scraper", "--name", name, "--address", address]
                p = subprocess.run(cmd, env=env, capture_output=True, text=True)
                output = (p.stdout or "") + (("\n" + p.stderr) if p.stderr else "")
        elif query:
            cmd = ["python", "-m", "florida_property_scraper", "--query", query]
            p = subprocess.run(cmd, env=env, capture_output=True, text=True)
            output = (p.stdout or "") + (("\n" + p.stderr) if p.stderr else "")
        else:
            output = "Enter either name+address or a single query."

    return render_template_string(HTML, query=query, name=name, address=address, output=output)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
