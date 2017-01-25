class Element(object):

    ''' A parsed XML element '''

    def __init__(self, name, attributes):
        '''Record tagname and attributes dictionary'''
        self.name = name
        self.attributes = attributes
        # Initialize the element's cdata and children to empty
        self.cdata = ''
        self.children = []

    def __str__(self):
        ''' convert element and children to string '''
        return self.toString()

    def addChild(self, element):
        '''Add a reference to a child element'''
        self.children.append(element)

    def getAttribute(self, key):
        '''Get an attribute value'''
        return self.attributes.get(key)

    def setAttribute(self, key, value):
        ''' Set an attribute value '''
        self.attributes[key] = value

    def getData(self):
        '''Get the cdata'''
        return self.cdata

    def setData(self, data):
        ''' Set the data '''
        self.cdata = data

    def hasElement(self, name):
        ''' return True if there is a child with given name,
            otherwise False '''
        for child in self.children:
            if child.name == name :
                return True
        return False

    def hasElementLike(self, name):
        ''' return True if there is a child which contains name,
            otherwise False '''
        for child in self.children:
            if child.name.find(name) >- 1 :
                return True
        return False

    def getElement(self, name):
        '''Get the element with name, if a list present return the first '''
        for child in self.children :
            if child.name == name :
                return child
        return None

    def getElements(self, name=''):
        '''Get a list of child elements'''
        if name:
            # return only those children with a matching tag name
            return [c for c in self.children if c.name == name]
        else:
            # no tag name is specified, return the all children
            return list(self.children)

    def getAllElements(self, element_array, name=''):
        ''' Get an array of all elements recursively '''
        if name:
            if self.name == name:
                 element_array.append(self)
        else:
            element_array.append(self)
        for c in self.children:
            c.getAllElements(element_array,name)

    def toString(self, level=0, indent=' '):
        ''' print element and children '''
        indent = indent * level
        retval = indent + "<%s" % self.name
        for attribute in self.attributes:
            retval += ' %s="%s"' % (attribute, self.attributes[attribute])
        content = ""
        for child in self.children:
            content += child.toString(level=level+1)
        if not content and not self.cdata :
            retval += "/>\n"
        else :
            # TODO formatting of output
            #retval += ">%s%s</%s>" % (escape(self.cdata), content, self.name)
            if content :
                content = "\n" + content + indent
            if self.cdata :
                content = '%s%s' % (self.cdata, content)
            retval += ">" + content + ("</%s>\n" % self.name)
        return retval