# uv add deepdiff --active
# uv run --active streamlit run .\app\streamlit\deepdiff_json.py
import streamlit as st
from deepdiff import DeepDiff

master_index = 0

jsons = [{"name": "Alice", "age": 25}, {"name": "Bob", "age": 26}, {"name": "Alice", "age": 27}]
cols = st.columns(len(jsons))

for i, col in enumerate(cols):
    _json = jsons[i]
    with col:
        st.subheader(_json['name'])
        if st.button(f"按钮 {i + 1}", key=f"btn_{i}"):
            st.write(f"你点击了按钮 {i + 1}")
        if i == master_index:
            st.json(_json)
        else:
            st.json(DeepDiff(jsons[master_index], _json, verbose_level=2))
