/*
 * Copyright (C) 2017 NFQ Solutions
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package com.nfqsolutions.jpalm;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.nfqsolutions.jpalm.core.ZmqContex;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.zeromq.ZContext;
import org.zeromq.ZMQ;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;

public class Client implements AutoCloseable {

    public class ClientException extends Exception
    {
        public ClientException(String message) {
            super(message);
        }

        public ClientException(String message, final Exception e) {
            super(message, e);
        }
    }

    private class Sender_Thread extends Thread {

        private final String function;
        private final Iterable<ByteString> generator;
        private final String cache;

        Sender_Thread(final String function, final Iterable<ByteString> generator,
                        final String cache) {

            this.function = function;
            this.generator = generator;
            this.cache = cache;
        }

        Sender_Thread(final String function, final ByteString item,
                      final String cache) {
            List<ByteString> l = new ArrayList<ByteString>(1);
            l.add(item);
            this.function = function;
            this.generator = l;
            this.cache = cache;
        }

        public void run() {
            sender();
        }

        private void sender() {
            final ZMQ.Socket socket = ctx.createSocket(ZMQ.PUSH);
            socket.connect(push_address);
            for(final ByteString payload : this.generator) {
                final Messages.PalmMessage.Builder messageBuilder = Messages.PalmMessage.newBuilder();
                messageBuilder.setPipeline(pipeline);
                messageBuilder.setClient(uuid.toString());
                messageBuilder.setStage(0);
                messageBuilder.setFunction(this.function);
                if(this.cache != null)
                    messageBuilder.setCache(this.cache);
                messageBuilder.setPayload(payload);
                socket.send(messageBuilder.build().toByteArray());
            }
        }
    }

    private final static Logger logger = Logger.getLogger(Client.class);

    private final static ZContext ctx = ZmqContex.getInstance();

    private final UUID uuid = UUID.randomUUID();

    private final byte[] uuidB;

    private final ZMQ.Socket db;

    private final String sub_address;

    private final String push_address;

    private final String server_name;

    private final String pipeline;

    private final boolean session_set;

    public UUID getUuid() {
        return uuid;
    }

    public ZMQ.Socket getDb() {
        return db;
    }

    public String getSub_address() {
        return sub_address;
    }

    public String getPush_address() {
        return push_address;
    }

    public String getServer_name() {
        return server_name;
    }

    public String getPipeline() {
        return pipeline;
    }

    /**
     *
     * @param server_name: Server you are connecting to.
     * @param db_address: Address for cache service, for first connectio or configuration.
     * @param push_address: Address of the push service of the server to pull from.
     * @param sub_address: Address of the pub service of the server to subscribe to.
     * @param pipeline: Name of the pipeline if the session has to be reused.
     *
     * @throws ClientException if something goes wrong
     */
    public Client(final String server_name,
                  final String db_address,
                  final String push_address,
                  final String sub_address,
                  final String pipeline) throws ClientException {
        this(server_name, db_address, push_address, sub_address, pipeline, Level.INFO, false, 1000);
    }

    /**
     *
     * @param server_name: Server you are connecting to.
     * @param db_address: Address for cache service, for first connectio or configuration.
     * @param push_address: Address of the push service of the server to pull from.
     * @param sub_address: Address of the pub service of the server to subscribe to.
     * @param pipeline: Name of the pipeline if the session has to be reused.
     * @param logging_level: Specify the logging level
     * @param this_config: Do not fetch configuration from server.
     *
     * @throws ClientException if something goes wrong
     */
    public Client(final String server_name,
                  final String db_address,
                  final String push_address,
                  final String sub_address,
                  final String pipeline,
                  final Level logging_level,
                  final boolean this_config) throws ClientException {
        this(server_name, db_address, push_address, sub_address, pipeline, logging_level, this_config, 1000);
    }

    /**
     *
     * @param server_name: Server you are connecting to.
     * @param db_address: Address for cache service, for first connectio or configuration.
     * @param push_address: Address of the push service of the server to pull from.
     * @param sub_address: Address of the pub service of the server to subscribe to.
     * @param pipeline: Name of the pipeline if the session has to be reused.
     * @param logging_level: Specify the logging level
     * @param this_config: Do not fetch configuration from server.
     * @param initializtion_time: Time in milliseconds to wait until finish initilization finish.
     * @throws ClientException if something goes wrong
     */
    public Client(final String server_name,
                  final String db_address,
                  final String push_address,
                  final String sub_address,
                  final String pipeline,
                  final Level logging_level,
                  final boolean this_config,
                  final long initializtion_time) throws ClientException {
        if(logging_level != null) {
            logger.setLevel(logging_level);
        }

        try {
            uuidB = this.uuid.toString().getBytes("utf-8");
        } catch (UnsupportedEncodingException e) {
            logger.warn("Couldn't set identity to database socket", e);
            throw new ClientException("Error encoding uuid", e);
        }

        this.server_name = server_name;

        if(db_address == null) {
            throw new ClientException("db_address can not be null");
        }

        // Init DB soccket
        this.db = ctx.createSocket(ZMQ.REQ);
        this.db.setIdentity(uuidB);
        this.db.connect(db_address);

        if(pipeline == null) {
            this.pipeline = UUID.randomUUID().toString();
            this.session_set = false;
        }
        else {
            this.pipeline = pipeline;
            this.session_set = true;
        }

        if(this_config && sub_address != null && push_address != null)
            logger.warn("Not fetching config from the server");
        else
            logger.info("Fetching configuration from the server");

        final String name = this.getString("name");
        if(!name.equals(this.server_name)) {
            throw new ClientException("You are connecting to the wrong server");
        }
        if(sub_address == null) {
            this.sub_address = this.getString("pub_address");
        }
        else {
            this.sub_address = sub_address;
        }
        if(push_address == null) {
            this.push_address = this.getString("pull_address");
        }
        else {
            this.push_address = sub_address;
        }

        logger.info("CLIENT" + uuid + " database address: " + db_address);
        logger.info("CLIENT" + uuid + " subscription address: " + this.sub_address);
        logger.info("CLIENT" + uuid + " push address: " + this.push_address);

        try {
            Thread.sleep(initializtion_time, 0);
        } catch (InterruptedException e) {
            logger.warn("CLIENT initialization interrupted" );
        }
    }

    /**
     * Clean the state of the client.
     */
    public void clean() {
        this.db.close();
    }

    /**
     * Close the state of the client cleaning.
     */
    public void close() throws Exception {
        this.clean();
    }

    private List<ByteString> recv_multipartList(final ZMQ.Socket sub_socket, final int messages) {
        final List<ByteString> result = new ArrayList<ByteString>(messages);
        for(final ByteString e : recv_multipart(sub_socket, messages))
            result.add(e);
        return result;
    }

    private List<String> recv_multipartStringList(final ZMQ.Socket sub_socket, final int messages) {
        final List<String> result = new ArrayList<String>(messages);
        for(final String e : recv_multipartString(sub_socket, messages))
            result.add(e);
        return result;
    }

    private Iterable<String> recv_multipartString(final ZMQ.Socket sub_socket, final int messages) {
        return new Iterable<String>(){
            @Override
            public Iterator<String> iterator() {
                return new Iterator<String>() {
                    private final Iterator<ByteString> iter = recv_multipart(sub_socket, messages).iterator();

                    @Override
                    public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override
                    public String next() {
                        return iter.next().toStringUtf8();
                    }

                    @Override
                    public void remove() {
                        iter.remove();
                    }
                };
            }
        };
    }

    private Iterable<ByteString> recv_multipart(final ZMQ.Socket sub_socket, final int messages) {
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
                        final String client = sub_socket.recvStr();

                        if (!sub_socket.hasReceiveMore())
                            throw new NoSuchElementException("Push socket has not receive second part.");
                        final Messages.PalmMessage msg;
                        try {
                            msg = Messages.PalmMessage.parseFrom(sub_socket.recv());
                        } catch (InvalidProtocolBufferException e) {
                            throw new NoSuchElementException("Error parsing message: " + e.getMessage());
                        }
                        if (sub_socket.hasReceiveMore())
                            throw new NoSuchElementException("Push socket has receive a not expected part.");

                        i++;
                        return msg.getPayload();
                    }

                    @Override
                    public void remove() {
                        throw new NotImplementedException();
                    }
                };
            }
        };
    }

    /**
     * Send Job
     *
     * @param function: function to execute
     * @param generator: data to provide
     * @param cache: cache
     * @param messages: number of messages
     *
     * @return result of the function
     */
    public Iterable<ByteString> job(final String function, final Iterable<ByteString> generator, final String cache, final int messages) {
        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        // Remember that sockets are not thread safe and runs in background.
        final Sender_Thread sender_thread = new Sender_Thread(function, generator, cache);
        sender_thread.start();

        return recv_multipart(sub_socket, messages);
    }

    public Iterable<ByteString> job(final String function, final Iterable<ByteString> generator, final String cache) {
        return job(function, generator, cache, Integer.MAX_VALUE);
    }

    public Iterable<String> jobString(final String function, final Iterable<ByteString> generator, final String cache, final int messages) {
        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        // Remember that sockets are not thread safe and runs in background.
        final Sender_Thread sender_thread = new Sender_Thread(function, generator, cache);
        sender_thread.start();

        return recv_multipartString(sub_socket, messages);
    }

    public Iterable<String> jobString(final String function, final Iterable<ByteString> generator, final int messages) {
        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        // Remember that sockets are not thread safe and runs in background.
        final Sender_Thread sender_thread = new Sender_Thread(function, generator, null);
        sender_thread.start();

        return recv_multipartString(sub_socket, messages);
    }

    /**
     * Send Job
     *
     * @param function: function to execute
     * @param item: data to provide
     * @param cache: cache
     * @param messages: number of messages
     *
     * @return result of the function
     */
    public Iterable<ByteString> job(final String function, final ByteString item, final String cache, final int messages) {
        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        // Remember that sockets are not thread safe and runs in background.
        final Sender_Thread sender_thread = new Sender_Thread(function, item, cache);
        sender_thread.start();

        return recv_multipart(sub_socket, messages);
    }

    public Iterable<ByteString> job(final String function, final ByteString item, final String cache) {
        return job(function, item, cache, Integer.MAX_VALUE);
    }

    public Iterable<String> jobString(final String function, final ByteString item, final String cache, final int messages) {
        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        // Remember that sockets are not thread safe and runs in background.
        final Sender_Thread sender_thread = new Sender_Thread(function, item, cache);
        sender_thread.start();

        return recv_multipartString(sub_socket, messages);
    }

    public Iterable<String> jobString(final String function, final ByteString item, final int messages) {
        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        // Remember that sockets are not thread safe and runs in background.
        final Sender_Thread sender_thread = new Sender_Thread(function, item, null);
        sender_thread.start();

        return recv_multipartString(sub_socket, messages);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function: function to execute
     * @param payload: data
     * @param messages number of messages
     * @param cache: cache
     * @throws ClientException if something wrong if something wrong
     *
     * @return List of results
     */
    public List<ByteString> eval(final String function, final ByteString payload,
                                 final int messages, final String cache) throws ClientException {
        final ZMQ.Socket push_socket = ctx.createSocket(ZMQ.PUSH);
        push_socket.connect(this.push_address);

        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        final Messages.PalmMessage.Builder messageBuilder = Messages.PalmMessage.newBuilder();
        messageBuilder.setPipeline(this.pipeline);  // For a set job, the pipeline is not important
        messageBuilder.setClient(this.uuid.toString());
        messageBuilder.setStage(0);
        messageBuilder.setFunction(function);
        messageBuilder.setPayload(payload);
        if (cache != null)
            messageBuilder.setCache(cache);
        push_socket.send(messageBuilder.build().toByteArray());

        return recv_multipartList(sub_socket, messages);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function: function to execute
     * @param payload: data
     * @param messages number of messages
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public List<ByteString> eval(final String function, final ByteString payload,
                                 final int messages) throws ClientException {
        return eval(function, payload, messages, null);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function: function to execute
     * @param payload: data
     * @param cache cache
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public ByteString eval(final String function, final ByteString payload,
                                 final String cache) throws ClientException {
        return eval(function, payload, 1, cache).get(0);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function: function to execute
     * @param payload: data
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public ByteString eval(final String function, final ByteString payload) throws ClientException {
        return eval(function, payload, 1, null).get(0);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     * @param messages number of messages
     * @param cache cache
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public List<ByteString> eval(final String function, final Object payload,
                                 final int messages, final String cache) throws ClientException {
        return eval(function, ByteString.copyFromUtf8(payload.toString()), messages, cache);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     * @param messages number of messages
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public List<ByteString> eval(final String function, final Object payload,
                                 final int messages) throws ClientException {
        return eval(function, ByteString.copyFromUtf8(payload.toString()), messages, null);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     * @param cache cache
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public ByteString eval(final String function, final Object payload,
                                 final String cache) throws ClientException {
        return eval(function, ByteString.copyFromUtf8(payload.toString()), 1, cache).get(0);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public ByteString eval(final String function, final Object payload) throws ClientException {
        return eval(function, ByteString.copyFromUtf8(payload.toString()), 1, null).get(0);
    }

    public List<String> evalString(final String function, final ByteString payload,
                                 final int messages, final String cache) throws ClientException {
        final ZMQ.Socket push_socket = ctx.createSocket(ZMQ.PUSH);
        push_socket.connect(this.push_address);

        final ZMQ.Socket sub_socket = ctx.createSocket(ZMQ.SUB);
        sub_socket.connect(this.sub_address);
        sub_socket.subscribe(this.uuidB);

        final Messages.PalmMessage.Builder messageBuilder = Messages.PalmMessage.newBuilder();
        messageBuilder.setPipeline(this.pipeline);  // For a set job, the pipeline is not important
        messageBuilder.setClient(this.uuid.toString());
        messageBuilder.setStage(0);
        messageBuilder.setFunction(function);
        messageBuilder.setPayload(payload);
        if (cache != null)
            messageBuilder.setCache(cache);
        push_socket.send(messageBuilder.build().toByteArray());

        return recv_multipartStringList(sub_socket, messages);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload data
     * @param messages number of messages
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public List<String> evalString(final String function, final ByteString payload,
                                 final int messages) throws ClientException {
        return evalString(function, payload, messages, null);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload data
     * @param cache cache
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public String evalString(final String function, final ByteString payload,
                                 final String cache) throws ClientException {
        return evalString(function, payload, 1, cache).get(0);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload data
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public String evalString(final String function, final ByteString payload) throws ClientException {
        return evalString(function, payload, 1, null).get(0);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     * @param messages number of messages
     * @param cache cache
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public List<String> evalString(final String function, final Object payload,
                                 final int messages, final String cache) throws ClientException {
        return evalString(function, ByteString.copyFromUtf8(payload.toString()), messages, cache);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     * @param messages number of messages
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public List<String> evalString(final String function, final Object payload,
                                 final int messages) throws ClientException {
        return evalString(function, ByteString.copyFromUtf8(payload.toString()), messages, null);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     * @param cache cache
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public String evalString(final String function, final Object payload,
                                 final String cache) throws ClientException {
        return evalString(function, ByteString.copyFromUtf8(payload.toString()), 1, cache).get(0);
    }

    /**
     *
     * Execute single evaluation.
     *
     * @param function function to execute
     * @param payload: must implement toString
     *
     * @return List of results
     * @throws ClientException if something wrong
     */
    public String evalString(final String function, final Object payload) throws ClientException {
        return evalString(function, ByteString.copyFromUtf8(payload.toString()), 1, null).get(0);
    }

    /**
     *
     * Sets a key value pare in the remote database. If the key is not set,
     * the function returns a new key. Note that the order of the arguments
     * is reversed from the usual
     *
     * @param value Value to be stored
     * @param key Key for the k-v storage. It use toString method.
     *
     * @return new key or the same key
     * @throws ClientException if something wrong
     */
    public String set(final ByteString value, final Object key) {
        final Messages.PalmMessage.Builder messageBuilder = Messages.PalmMessage.newBuilder();
        messageBuilder.setPipeline(UUID.randomUUID().toString());  // For a set job, the pipeline is not important
        messageBuilder.setClient(this.uuid.toString());
        messageBuilder.setStage(0);
        messageBuilder.setFunction(this.server_name + ".set");
        messageBuilder.setPayload(value);
        if(key != null && this.session_set)
            messageBuilder.setCache(this.pipeline + key.toString());
        else if(key != null)
            messageBuilder.setCache(key.toString());

        this.db.send(messageBuilder.build().toByteArray());
        return ByteString.copyFrom(this.db.recv()).toStringUtf8();
    }

    public String set(final byte[] value, final Object key) {
        return set(ByteString.copyFrom(value), key);
    }

    public String set(final String value, final Object key) {
        return set(ByteString.copyFromUtf8(value), key);
    }

    public String set(final ByteBuffer value, final Object key) {
        return set(ByteString.copyFrom(value), key);
    }

    public String set(final InputStream value, final Object key) throws ClientException {
        try {
            return set(ByteString.readFrom(value), key);
        } catch (IOException e) {
            throw new ClientException("IO error readgin from InputStream", e);
        }
    }

    /**
     *
     * Gets a value from server's internal cache.
     *
     * @param key Key for the k-v storage. It use toString method.
     *
     * @return new value.
     */
    public byte[] get(final Object key) {
        final Messages.PalmMessage.Builder messageBuilder = Messages.PalmMessage.newBuilder();
        messageBuilder.setPipeline(UUID.randomUUID().toString());  // For a set job, the pipeline is not important
        messageBuilder.setClient(this.uuid.toString());
        messageBuilder.setStage(0);
        messageBuilder.setFunction(this.server_name + ".get");
        messageBuilder.setPayload(ByteString.copyFromUtf8(key.toString()));

        this.db.send(messageBuilder.build().toByteArray());
        return this.db.recv();
    }

    public ByteString getByteString(final Object key) throws ClientException {
        return ByteString.copyFrom(get(key));
    }

    public String getString(final Object key) throws ClientException {
        return getByteString(key).toStringUtf8();
    }

    public ByteBuffer getByteBuffer(final Object key) throws ClientException {
        return ByteBuffer.wrap(get(key));
    }

    /**
     *
     * Deletes data in the server's internal cache.
     *
     * @param key Key for the k-v storage. It use toString method.
     *
     */
    public void delete(final Object key) {
        final Messages.PalmMessage.Builder messageBuilder = Messages.PalmMessage.newBuilder();
        messageBuilder.setPipeline(UUID.randomUUID().toString());  // For a set job, the pipeline is not important
        messageBuilder.setClient(this.uuid.toString());
        messageBuilder.setStage(0);
        messageBuilder.setFunction(this.server_name + ".delete");
        messageBuilder.setPayload(ByteString.copyFromUtf8(key.toString()));

        this.db.send(messageBuilder.build().toByteArray());
        this.db.recv();
    }
}
