from icecream import ic
import streamlit as st
from datetime import datetime
import pymilvus
from transformers import AutoTokenizer, AutoModel
import torch
from jproperties import Properties

configs = Properties()
with open('milvus-sink-connector.properties', 'rb') as config_file:
    configs.load(config_file)

VECTOR_DIMENSIONS=384

tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

def hf_document_embed(chunk, tokenizer, model, torch, length=384):
    inputs = tokenizer(
        chunk, padding=True, truncation=True, return_tensors="pt", max_length=length
    )
    with torch.no_grad():
        embed = model(**inputs).last_hidden_state[:, 0].cpu().detach().numpy()
    return embed.flatten()

with st.form("my_form"):
    st.image("banner.png")
    text_val = st.text_area("What's going on at Hacker News today?",value="What's new for Play Station?")

    submitted = st.form_submit_button("Go!")
    if submitted:
        pymilvus.connections.connect(uri=configs["public.endpoint"].data, token=configs["token"].data)
        collection = pymilvus.Collection(configs["collection.name"].data)

        embedding = hf_document_embed(
            text_val, tokenizer, model, torch, length=VECTOR_DIMENSIONS
        )

        results = collection.search(data=[embedding], anns_field="doc_embedding", param={'level':3}, limit=10, output_fields=["by", "time","text"], expr=f'type == "comment"', consistency_level="Strong")

        with st.container(height=500):
            for r in results[0]:
                matching = r.entity
                time_diff = datetime.now() - datetime.fromtimestamp(matching.get('time'))
                minutes_ago = divmod(time_diff.total_seconds(), 60)[0]
                time_display = datetime.fromtimestamp(matching.get('time'))
                if minutes_ago < 120:
                    time_display = f"{int(minutes_ago)} minutes ago"
                st.markdown(f"{time_display} - {matching.get('by')} 🗣️\n> {matching.get('text')}")
                st.slider("distance:",min_value=0.0, max_value=1.0,value=r.distance,key=r.id,disabled=True,label_visibility="collapsed")
