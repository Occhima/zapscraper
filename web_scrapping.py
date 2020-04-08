from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.firefox.options import Options
import numpy as np
from validators import url
from urllib.error import HTTPError
from tqdm import tqdm
import pandas as pd
import os

import csv


class WebScrapper:

    def __init__(self, urls):

        if not isinstance(urls, (list, np.ndarray, pd.Series)):
            raise TypeError(f"Given URL type: {type(urls)} is not valid")

        for ur_l in urls:
            if not url(ur_l):
                raise TypeError(f" the url {ur_l} is not valid")

        # Creating the data dir
        print(f"Creating the 'data' dir for dumping...\n")
        os.mkdir("data")
        print("DONE!\n")

        # Configuando o bot que abre arquivos web automaticamente
        options = Options()
        options.headless = True
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')

        #  Geckodriver exe path, if not added to PATH
        executable_path = r'PATH_TO_GECKO'
        self.driver = webdriver.Firefox(options=options, executable_path=executable_path)
        self.urls = urls
        self.df = np.array([])

        # ZAP imoveis XPath to the data we're scraping
        self.xpath = '/html/body/main/section/div/div/section'
        self.file_path = 'data/result_data.csv'

    def get_data(self):
        dr = self.driver
        print("\n\t Loop Initialization \n", end='-' * 50)
        print("\n")

        with open(self.file_path) as f:
            with csv.writer(f, delimiter=',') as csv_writter:
                for ur_l in tqdm(self.urls):

                    dr.get(ur_l)
                    print(f"\t\nIncializando o scrapping para a categoria: {ur_l[38: 48]}\n")
                    it = 1

                    while True:
                        try:
                            data = dr.find_element_by_xpath(self.xpath)
                            data = data.get_attribute('outerHTML')
                            soup = BeautifulSoup(data, 'html.parser')

                            # Fetching aquired data
                            data = soup.findAll("div", {"class": ["simple-card__prices simple-card__listing-prices",
                                                                  "simple-card__actions"]})
                            conc = [hit.text.strip().split('\n') for hit in data]
                            conc = np.array([conc[i] + conc[i + 1] for i in range(len(conc) - 1)])

                            # Creating a numerical numpy array from the scraped data
                            self.df = np.concatenate((self.df, conc), axis=None)

                        # For 404 error not found, it meand we've reached page limit
                        except HTTPError as http_error:
                            print(f"Completed the process for the url{ur_l}, Fetched data from {it} pages\n")
                            # print(http_error)
                            break

                        except NoSuchElementException:
                            print(f"Page Limit exceded, moving on to the next page\n")
                            break

                        finally:
                            # Printing results for the user
                            print(f"\nData aquisition from the page number {it} complete\n")
                            print(f"\tArray dimensions: {self.df.shape}\n \t Data size: { (self.df.size * self.df.itemsize)}b\n")
                            print(f"Writing data in the csv...\n")

                            # parsing data to a csv file, automatically save data
                            csv_writter.writerows(conc)
                            print("\tDone! Moving on to the next page...\n")

                            # moving on to the next page
                            it += 1
                            dr.get(ur_l[:-1] + str(it))

        # Closing file and driver
        dr.quit()
        f.close()
        return self.df


def main():

    # Testing the object
    urls = [
        'https://www.zapimoveis.com.br/aluguel/loja-salao/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Loja%20%2F%20Sal%C3%A3o%20%2F%20Ponto%20Comercial&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/conjunto-comercial-sala/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Conjunto%20Comercial%20%2F%20Sala&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/casa-comercial/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Casa%20Comercial&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/hoteis-moteis-pousadas/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Hotel%20%2F%20Motel%20%2F%20Pousada&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/andares-lajes-corporativas/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Andar%20%2F%20Laje%20Corporativa&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/predio-inteiro/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Pr%C3%A9dio%20Inteiro&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/terrenos-lotes-comerciais/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Terrenos%20%2F%20Lotes%20Comerciais&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/galpao-deposito-armazem/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Galp%C3%A3o%20%2F%20Dep%C3%B3sito%20%2F%20Armaz%C3%A9m&tipo=Im%C3%B3vel%20usado&pagina=1',
        'https://www.zapimoveis.com.br/aluguel/box-garagem/sp+sao-paulo/?onde=,S%C3%A3o%20Paulo,S%C3%A3o%20Paulo,,,,BR%3ESao%20Paulo%3ENULL%3ESao%20Paulo,-23.533371,-46.791451&transacao=Aluguel&tipoUnidade=Comercial,Garagem&tipo=Im%C3%B3vel%20usado&pagina=1']

    sc = WebScrapper(urls=urls)
    print("-" * 50, end='\n\n')
    print("Initializing Web Scraper\n\n", end="-" * 50)
    d = sc.get_data()


if __name__ == "__main__":
    main()