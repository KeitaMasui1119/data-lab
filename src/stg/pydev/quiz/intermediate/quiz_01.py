class Book:
    """A class representing a book with title, author, and price."""
    def __init__(self, title:str, author:str, price:float):
        self.title = title
        self.author = author
        self.price = price

    def __str__(self):
        return f"{self.title} by {self.author} - ${self.price}"

    def __eq__(self, other):
        if not isinstance(other, Book):
            return NotImplemented
        return (self.title == other.title and
                self.author == other.author)

    def discount(self, rate:float):
        """Apply a discount to the book's price."""
        if 0 < rate < 1:
            self.price *= (1 - rate)
        else:
            raise ValueError("Discount rate must be between 0 and 1.")

def test():
    book = Book(title="1984", author="George Orwell", price=9.99)

    book2 = Book(title="1984", author="George Keita", price=12.99)

    print(book)

    print(book2 == book)

    print(f"Original Price: ${book.price} -> Discouted Price: ${book.discount(0.2)}")

if __name__ == "__main__":
    test()
