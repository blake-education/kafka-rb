# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
module Kafka
  module IO
    attr_accessor :socket, :host, :port, :ssl_config

    def connect(host, port, ssl_config=nil)
      raise ArgumentError, "No host or port specified" unless host && port
      self.host = host
      self.port = port
      self.ssl_config = ssl_config
      self.socket = create_socket(host, port, ssl_config)
    end

    def reconnect
      self.socket = create_socket(self.host, self.port, self.ssl_config)
    rescue
      self.disconnect
      raise
    end

    def create_socket(host, port, ssl_config)
      socket = TCPSocket.new(host, port)
      if ssl_config
        wrap_socket_with_ssl(socket, ssl_config)
      else
        socket
      end
    end

    def wrap_socket_with_ssl(socket, ssl_config)
      ssl_config = {} unless Hash === ssl_config
      if ctx = ssl_config[:context]
        OpenSSL::SSL::SSLSocket.new(socket, ctx)
      else
        OpenSSL::SSL::SSLSocket.new(socket)
      end
    end

    def disconnect
      self.socket.close rescue nil
      self.socket = nil
    end

    def read(length)
      self.socket.read(length) || raise(SocketError, "no data")
    rescue
      self.disconnect
      raise SocketError, "cannot read: #{$!.message}"
    end

    def write(data)
      self.reconnect unless self.socket
      self.socket.write(data)
    rescue
      self.disconnect
      raise SocketError, "cannot write: #{$!.message}"
    end

  end
end
