tingodb
=======

In-process pure javascript mongodb compatible database

!!! NOTE: WORK IN PROGRESS, NO RELEASES YET

For some of our nodejs projects we need in-process document based database. We tried some existing and seriously considered [alfred](http://pgte.github.io/alfred/) for this purpose. After many fixes we get it working, but it still very unstable mostly because of some features we not expect from in-process database (replication for instance). So at some moment we decided to build our own engine.

We still not sure that in-process database can be sufficient for mentioned project and decided to build if forward compatibly with mongodb API. I.e. we build our application with dual approach so it can work with mongodb and tingodb which can be switched just in configuration.

So far basic functionality is ready and lot of tests are passed including some contributed direclty from mongodb nodejs driver. 

## MIT License

Copyright (c) [PushOk Software](http://www.pushok.com)

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
