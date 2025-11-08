# uvx streamlit run ./app/streamlit/<this>.py
import streamlit as st
import difflib

text1 = st.text_area("原始文本")
text2 = st.text_area("修改后文本")

diff_lines = difflib.unified_diff(
    text1.splitlines(), text2.splitlines(),
    fromfile='原始', tofile='修改后', lineterm=''
)
# git bash style
st.markdown("```diff\n" + "\n".join(diff_lines) + "\n```")