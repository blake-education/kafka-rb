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
    def self.use_ssl=(flag)
      @use_ssl = flag 

      if use_ssl?
        require 'openssl'
      end

      @use_ssl
    end
    def self.use_ssl?; @use_ssl end
    def self.ssl_context=(ctx); @ssl_context = ctx end
    def self.ssl_context; @ssl_context end

    attr_accessor :socket, :host, :port

    def connect(host, port)
      raise ArgumentError, "No host or port specified" unless host && port
      self.host = host
      self.port = port
      self.socket = create_socket(host, port)
    end

    def reconnect
      self.socket = create_socket(self.host, self.port)
    rescue
      self.disconnect
      raise
    end

    def create_socket(host, port)
      socket = TCPSocket.new(host, port)
      if Kafka::IO.use_ssl?
        wrap_socket_with_ssl(socket)
      else
        socket
      end
    end

    def wrap_socket_with_ssl(socket)
      OpenSSL::SSL::SSLSocket.new(socket, Kafka::IO.ssl_context)
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
