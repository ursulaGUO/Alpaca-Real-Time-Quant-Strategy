from transformers import pipeline

# Naming pipeline 
classifier = pipeline("sentiment-analysis") 

test = classifier("Apple stock is going to tank today. \
                  Do not anticipate it will recover soon")

for i in test:
    print(f"sentiment: {i['label']}, score: {i['score']}")