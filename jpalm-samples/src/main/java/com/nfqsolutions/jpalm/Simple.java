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

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;

public class Simple {
    private final static Logger logger = Logger.getLogger(Client.class);

    private final static String server_name = "my_server";
    private final static String db_address = "tcp://127.0.0.1:5555";
    private final static String push_address = null;
    private final static String sub_address = null;
    private final static String pipeline = null;
    private final static Level logging_level = Level.INFO;

    public static void main( String[] args ) throws Client.ClientException, UnsupportedEncodingException {
        final Client client = new Client( server_name, db_address, push_address,
                                            sub_address, pipeline, logging_level,
                                            false, 2500);


        System.out.println("Example eval");
        System.out.println("Client got: " + client.evalString("my_server.foo", "a message"));
        System.out.println("Client got: " + client.eval("my_server.foo", "a message").toStringUtf8());

        System.out.println("Example cache");
        client.set("cached data", "cached");
        System.out.println("Client get cache: " + client.getString("cached"));
    }
}
