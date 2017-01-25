from .element import Element
from xml.parsers import expat

class Xml2Object(object):

    ''' XML to Object converter '''

    def __init__(self, encoding='', strip_data=True):
        ''' initialize '''
        self.root = None
        self.nodeStack = []
        self.encoding = encoding
        self.strip_data = strip_data

    def StartElement(self, name, attributes):
        '''Expat start element event handler'''
        # Instantiate an Element object
        if self.encoding :
            element = Element(name.encode(self.encoding), attributes)
        else:
            element = Element(name, attributes)
        # Push element onto the stack and make it a child of parent
        if self.nodeStack:
            parent = self.nodeStack[-1]
            parent.addChild(element)
        else:
            self.root = element
        self.nodeStack.append(element)

    def EndElement(self, name):
        '''Expat end element event handler'''
        self.nodeStack.pop()

    def CharacterData(self, data):
        '''Expat character data event handler'''
        if not self.strip_data or data.strip():
            element = self.nodeStack[-1]
            if self.encoding :
                element.cdata += data.encode(self.encoding)
            else :
                element.cdata += data

    def Parse(self, filename, xml=None):
        ''' Create an Expat parser, input can be a filename or the xml
        '''
        Parser = expat.ParserCreate()
        # Set the Expat event handlers to our methods
        Parser.StartElementHandler  = self.StartElement
        Parser.EndElementHandler    = self.EndElement
        Parser.CharacterDataHandler = self.CharacterData
        # Parse the XML File
        if filename :
            xml = open(filename).read()
            if self.encoding :
                xml = xml.decode(self.encoding)
        ParserStatus = Parser.Parse(xml, 1)
        return self.root
