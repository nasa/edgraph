import json
import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from tqdm import tqdm
from collections import Counter

# Check if a GPU is available and how many GPUs are available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
n_gpus = torch.cuda.device_count()
print(f"Using {n_gpus} GPUs")

# Set up the Hugging Face model and tokenizer
model_name = "arminmehrabian/nasa-impact-nasa-smd-ibm-st-v2-classification-finetuned"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSequenceClassification.from_pretrained(model_name)

# Use DataParallel for multi-GPU support if multiple GPUs are available
if n_gpus > 1:
    model = torch.nn.DataParallel(model)

# Move the model to the device (GPU if available)
model.to(device)

# Define the classification function
def classify_abstract(abstract_text):
    inputs = tokenizer(abstract_text, return_tensors="pt", truncation=True, padding=True, max_length=512)
    inputs = {key: value.to(device) for key, value in inputs.items()}  # Move input tensors to the same device as the model
    with torch.no_grad():
        logits = model(**inputs).logits
    predicted_class_id = torch.argmax(logits, dim=-1).item()
    return predicted_class_id

# Label mapping for the applied research areas (ARAs) - All uppercase
label_mapping = {
    0: "AGRICULTURE",
    1: "AIR QUALITY",
    2: "ATMOSPHERIC/OCEAN INDICATORS",
    3: "CRYOSPHERIC INDICATORS",
    4: "DROUGHTS",
    5: "EARTHQUAKES",
    6: "ECOSYSTEMS",
    7: "ENERGY PRODUCTION/USE",
    8: "ENVIRONMENTAL IMPACTS",
    9: "FLOODS",
    10: "GREENHOUSE GASES",
    11: "HABITAT CONVERSION/FRAGMENTATION",
    12: "HEAT",
    13: "LAND SURFACE/AGRICULTURE INDICATORS",
    14: "PUBLIC HEALTH",
    15: "SEVERE STORMS",
    16: "SUN-EARTH INTERACTIONS",
    17: "VALIDATION",
    18: "VOLCANIC ERUPTIONS",
    19: "WATER QUALITY",
    20: "WILDFIRES"
}

# Clean up abstract text by removing unnecessary HTML-like tags
def clean_abstract(abstract):
    return abstract.replace("<SUB>", "").replace("</SUB>", "").replace("<SUP>", "").replace("</SUP>", "").replace("<", "").replace(">", "")

# Process and classify each publication's abstract
def classify_publications(publications, label_mapping):
    classified_publications = {}
    successfully_classified = 0

    for doi, entries in tqdm(publications.items(), desc="Classifying publications"):
        for entry in entries:
            abstract = clean_abstract(entry.get('abstract', ''))
            predicted_class_id = classify_abstract(abstract)
            predicted_label = label_mapping[predicted_class_id]
            entry['classified_area'] = predicted_label  # Add the classified label to the entry

            if doi not in classified_publications:
                classified_publications[doi] = []
            classified_publications[doi].append(entry)

            successfully_classified += 1

    return classified_publications, successfully_classified

# Save the classified publications with the new "classified_area" field
def save_classified_publications(classified_publications, output_file_path):
    with open(output_file_path, "w") as f:
        json.dump(classified_publications, f, indent=4)

# Generate statistics on the classification counts
def log_classification_statistics(classified_publications):
    labels = [entry['classified_area'] for entries in classified_publications.values() for entry in entries]
    label_count = Counter(labels)
    
    print("\nClassification Counts:")
    for label, count in label_count.items():
        print(f"{label}: {count}")

# Main execution flow
def main():
    # Input file path - change this to point to your JSON file
    input_file_path = "citing_publications.json"
    output_path = "classified_publications_with_abstracts.json"  # Modify the path as necessary

    # Load the publications abstracts
    with open(input_file_path, "r") as f:
        publications = json.load(f)
    
    # Classify the abstracts and get stats
    classified_publications, successfully_classified = classify_publications(publications, label_mapping)

    # Save the classified publications to a new file
    save_classified_publications(classified_publications, output_path)

    # Log the classification statistics
    print(f"Successfully classified {successfully_classified} publication abstracts.")
    log_classification_statistics(classified_publications)

if __name__ == "__main__":
    main()
