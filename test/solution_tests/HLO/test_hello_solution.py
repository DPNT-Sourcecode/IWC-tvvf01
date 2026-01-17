from solutions.HLO.hello_solution import HelloSolution


class TestSum():
    def test_hello(self):
        assert HelloSolution.hello(self, "James") == "Hello, James!"


