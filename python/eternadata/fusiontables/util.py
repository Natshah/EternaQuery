import client as ft


def upload_csv(table_id, csv_files):
    if type(csv_files) == str:
        csv_files = [csv_files]
    ftclient = ft.FTClientOAuth2()
    #ftable.save_copy()
    for csv_file in csv_files:
		ftclient.Table(table_id).import_rows(csv_file)
    return True
    
  
  
  