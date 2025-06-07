import pandas as pd
import os

file_path = "../data/raw/"

list_dfs = []

class Transformer:
    """
    Transform class to handle the transformation of raw data files into a cleaned and structured format.
    """

    def __init__(self, input_path, output_file):
        self.input_path = input_path
        self.output_file = output_file
        self.dataframes = []
        os.makedirs(os.path.dirname(output_file), exist_ok=True)


    def process_all_files(self):
        """
        Process all files in the input directory, transforming them into a structured format.
        """

        for file in os.listdir(self.input_path):  # List files in the directory to ensure the file exists
            if not file.endswith(".ods"):
                continue
            servico = file.split("2")[0]

            df = self._process_file(file, servico)
            self.dataframes.append(df)


    def _process_file(self, file, servico):
        """
        Process a single file, transforming it into a structured DataFrame.
        """
        # Read the header row to identify and drop empty columns
        h_row = pd.read_excel(file_path+file, engine="odf", skiprows=8, nrows=1)
        empty_cols_mask = h_row.isna().all()  # Identify columns that are completely empty
        cols_to_drop = h_row.columns[empty_cols_mask]  # Get the names of those columns
        h_row = h_row.drop(columns=cols_to_drop)  # Drop those columns


        df = pd.read_excel(file_path+file, engine="odf", skiprows=8)
        df = df.dropna(axis=0, how="all")  # Drop rows that are completely empty
        df = df.drop(columns=cols_to_drop)  # Drop the same empty columns from h_row
        df = df.dropna() # Drop rows with any NaN values
        
        # print(df.head(10))  # Print the first 10 rows of the DataFrame for debugging

        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].str.lower().str.strip()  # Normalize string columns


        df_long = df.melt(
            id_vars=["GRUPO ECONÔMICO", "VARIÁVEL"],
            var_name="data",
            value_name="valor")

        df_long["data"] = pd.to_datetime(df_long["data"], format="%Y-%m").dt.to_period("M")

        df_long["valor"] = df_long["valor"].replace('-', 0)  # Ensure 'valor' is float

        df_long.insert(2, "SERVIÇO", servico)  # Insert the 'serviço' column at the beginning

        # df_long.to_csv(f"/data/processed/{file}.csv", index=False) # Uncomment to save each file individually

        return df_long


    def save_final_dataset(self):
        """
        Save the final concatenated DataFrame to a CSV file.
        """
        df_final = pd.concat(self.dataframes, ignore_index=True)
        df_final.to_csv(self.output_file, index=False)
        print(f"Final dataset saved as {self.output_file}")


    def run(self):
        """
        Run the transformation process.
        """
        self.process_all_files()
        self.save_final_dataset()

