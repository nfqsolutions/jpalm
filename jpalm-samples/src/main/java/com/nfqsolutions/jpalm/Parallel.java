/*
 *  Copyright (C) 2017 NFQ Solutions
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.nfqsolutions.jpalm;


import com.google.protobuf.ByteString;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.UnsupportedEncodingException;
import java.util.Iterator;

public class Parallel {
    private final static Logger logger = Logger.getLogger(Client.class);

    private final static String server_name = "server";
    private final static String db_address = "tcp://127.0.0.1:5559";
    private final static String push_address = null;
    private final static String sub_address = null;
    private final static String pipeline = null;
    private final static Level logging_level = Level.INFO;

    private static Iterable<ByteString> repeat(final String message, final int messages) {
        return new Iterable<ByteString>(){
            @Override
            public Iterator<ByteString> iterator() {
                return new Iterator<ByteString>() {
                    private int i = 0;
                    @Override
                    public boolean hasNext() {
                        return i<messages;
                    }

                    @Override
                    public ByteString next() {
                        i++;
                        return ByteString.copyFromUtf8(message);
                    }

                    @Override
                    public void remove() {
                        throw new NotImplementedException();
                    }
                };
            }
        };
    }

    public static void main( String[] args ) throws Client.ClientException, UnsupportedEncodingException {
        final Client client = new Client( server_name, db_address, push_address,
                sub_address, pipeline, logging_level,
                false);

        System.out.println("Example cache");
        client.set("cached data", "cached");
        System.out.println("Client get cache: " + client.getString("cached"));

        System.out.println("Example job");
        for( final String response : client.jobString("server.foo", repeat("a message", 10), 10) )
            System.out.println(response);
    }
}
