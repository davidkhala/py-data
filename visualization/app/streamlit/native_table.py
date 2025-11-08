import streamlit as st
import difflib

text1 = st.text_area("原始文本", "This is the original text.\nIt has two lines.")
text2 = st.text_area("修改后文本", "This is the modified text.\nIt has three lines.")

diff = difflib.HtmlDiff().make_table(
    text1.splitlines(), text2.splitlines(),
    fromdesc='原始', todesc='修改后',
    context=True, numlines=2
)

styled_html = f"""
<style>
table.diff {{ background-color: white; color: black; font-family: monospace; }}
td.diff_header {{ background-color: #e0e0e0; }}
td.diff_next {{ background-color: #f0f0f0; }}
td.diff_add {{ background-color: #aaffaa; }}
td.diff_chg {{ background-color: #ffffaa; }}
td.diff_sub {{ background-color: #ffaaaa; }}
</style>
{diff}
"""
# No comparison, just list out
st.components.v1.html(styled_html, scrolling=True)