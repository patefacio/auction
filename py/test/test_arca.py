from auction.parser.arca_parser import int_price

def testIntPrice():
    assert(200200000 == int_price("200.20"))
    assert(200000 == int_price(".2000"))
    assert(200000 == int_price("0.2000"))
    assert(1200000 == int_price("1.2000"))
    assert(11000000 == int_price("11"))
    assert(3140000 == int_price("3.14"))
