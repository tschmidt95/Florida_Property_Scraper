from pathlib import Path


def test_web_template_contains_demo_checkbox():
    p = Path('web_app.py')
    s = p.read_text()
    assert 'name="demo"' in s


def test_web_template_contains_demo_notice_or_pre():
    p = Path('web_app.py')
    s = p.read_text()
    # Either the demo notice or at least the output pre block should exist
    assert ('Running in <strong>DEMO</strong>' in s) or ('<pre' in s)
