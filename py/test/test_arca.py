from auction.parser.arca_parser import intPrice

def testIntPrice():
    assert((2, 20020) == intPrice("200.20"))
    assert((4, 2000) == intPrice(".2000"))
    assert((0, 11) == intPrice("11"))
