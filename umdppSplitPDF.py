import os
import tkinter as tk
from tkinter import filedialog, messagebox
from PyPDF2 import PdfReader, PdfWriter

# === Predefined titles ===
titles = [
    "Breast Cancer Screening",
    "Colorectal Cancer Screening",
    "Tobacco Screening and Cessation",
    "Depression Screening",
    "Controlling HbA1c",
    "Controlling High Blood Pressure",
    "Statin Therapy",
    "BMI Screening (Adults)",
    "Medication Reconciliation",
    "30 Day Readmission",
    "Opioids High Dosage",
    "Childhood Immunizations (Combo 7)",
    "Well Child Visits (3-6 Years)",
    "Well Child Visits (0-15 Months)",
    "Well Child Visits (15-30 Months)",
    "Depression Screening (Adolescents)",
    "BMI Screening (Adolescents)",
    "Post Partum Depression Screening",
    "Childhood Immunizations (Combo 10)",
    "Immunization for Adolescents"
]

def select_pdf():
    file_path = filedialog.askopenfilename(
        title="Select PDF File",
        filetypes=[("PDF files", "*.pdf")]
    )
    return file_path

def select_output_folder():
    folder_path = filedialog.askdirectory(title="Select Output Folder")
    return folder_path

def split_pdf(input_pdf, output_folder, titles):
    reader = PdfReader(input_pdf)
    num_pages = len(reader.pages)

    if len(titles) < num_pages:
        messagebox.showerror(
            "Error",
            f"Title list has {len(titles)} entries but PDF has {num_pages} pages."
        )
        return

    for i in range(num_pages):
        writer = PdfWriter()
        writer.add_page(reader.pages[i])

        output_filename = f"{titles[i]}.pdf"
        output_path = os.path.join(output_folder, output_filename)

        with open(output_path, "wb") as f_out:
            writer.write(f_out)

    messagebox.showinfo("Success", f"PDF split into {num_pages} files.")

def main():
    root = tk.Tk()
    root.withdraw()  # Hide root window

    input_pdf = select_pdf()
    if not input_pdf:
        return

    output_folder = select_output_folder()
    if not output_folder:
        return

    split_pdf(input_pdf, output_folder, titles)

if __name__ == "__main__":
    main()
