class BaseScraper:
    def fetch(self):
        raise NotImplementedError

    def parse(self, raw):
        raise NotImplementedError

    def save(self, data):
        raise NotImplementedError

    def run(self):
        raw = self.fetch()
        data = self.parse(raw)
        self.save(data)
