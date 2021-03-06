/*
 * Copyright 2012 Database Lab., Korea Univ.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tajo.rpc;

import com.google.protobuf.BlockingService;
import com.google.protobuf.Descriptors.MethodDescriptor;
import com.google.protobuf.Message;
import com.google.protobuf.RpcController;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jboss.netty.channel.*;
import tajo.rpc.RpcProtos.RpcRequest;
import tajo.rpc.RpcProtos.RpcResponse;

import java.lang.reflect.Method;
import java.net.InetSocketAddress;

public class ProtoBlockingRpcServer extends NettyServerBase {
  private static Log LOG = LogFactory.getLog(ProtoBlockingRpcServer.class);
  private final BlockingService service;
  private final ChannelPipelineFactory pipeline;

  public ProtoBlockingRpcServer(final Class<?> protocol,
                                final Object instance,
                                final InetSocketAddress bindAddress)
      throws Exception {

    super(bindAddress);

    String serviceClassName = protocol.getName() + "$" +
        protocol.getSimpleName() + "Service";
    Class<?> serviceClass = Class.forName(serviceClassName);
    Class<?> interfaceClass = Class.forName(serviceClassName +
        "$BlockingInterface");
    Method method = serviceClass.getMethod(
        "newReflectiveBlockingService", interfaceClass);

    this.service = (BlockingService) method.invoke(null, instance);
    this.pipeline = new ProtoPipelineFactory(new ServerHandler(),
        RpcRequest.getDefaultInstance());

    super.init(this.pipeline);
  }

  private class ServerHandler extends SimpleChannelUpstreamHandler {

    @Override
    public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
        throws Exception {

      final RpcRequest request = (RpcRequest) e.getMessage();

      String methodName = request.getMethodName();
      MethodDescriptor methodDescriptor =
          service.getDescriptorForType().findMethodByName(methodName);

      if (methodDescriptor == null) {
        throw new RemoteCallException(request.getId(),
            new NoSuchMethodException(methodName));
      }

      Message paramProto = null;
      if (request.hasRequestMessage()) {
        try {
          paramProto = service.getRequestPrototype(methodDescriptor)
              .newBuilderForType().mergeFrom(request.getRequestMessage()).
                  build();

        } catch (Throwable t) {
          throw new RemoteCallException(request.getId(), methodDescriptor, t);
        }
      }

      Message returnValue;
      RpcController controller = new NettyRpcController();

      try {
        returnValue = service.callBlockingMethod(methodDescriptor,
            controller, paramProto);
      } catch (Throwable t) {
        throw new RemoteCallException(request.getId(), methodDescriptor, t);
      }

      RpcResponse.Builder builder =
          RpcResponse.newBuilder().setId(request.getId());

      if (returnValue != null) {
        builder.setResponseMessage(returnValue.toByteString());
      }

      if (controller.failed()) {
        builder.setErrorMessage(controller.errorText());
      }

      e.getChannel().write(builder.build());
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
      if (e.getCause() instanceof RemoteCallException) {
        RemoteCallException callException = (RemoteCallException) e.getCause();
        e.getChannel().write(callException.getResponse());
      }

      throw new RemoteException(e.getCause());
    }
  }
}