#pragma once

#include <common/types.h>

#include <Poco/JSON/Object.h>

#include <memory>
#include <unordered_map>

namespace DB
{
struct Node
{
    /// Node identity. UUID
    String identity;

    /// `host` is network reachable like hostname, FQDN or IP
    String host;

    /// `channel` is the identifier of the host exposed to client for polling status
    String channel;

    String roles;

    Int16 https_port = -1;
    Int16 http_port = -1;
    Int16 tcp_port_secure = -1;
    Int16 tcp_port = -1;

    Node(const String & identity_, const std::unordered_map<String, String> & headers) : identity(identity_)
    {
        auto iter = headers.find("_host");
        if (iter != headers.end())
        {
            host = iter->second;
        }

        iter = headers.find("_channel");
        if (iter != headers.end())
        {
            channel = iter->second;
        }

        iter = headers.find("_node_roles");
        if (iter != headers.end())
        {
            roles = iter->second;
        }

        iter = headers.find("_https_port");
        if (iter != headers.end())
        {
            https_port = std::stoi(iter->second);
        }

        iter = headers.find("_http_port");
        if (iter != headers.end())
        {
            http_port = std::stoi(iter->second);
        }

        iter = headers.find("_tcp_port_secure");
        if (iter != headers.end())
        {
            tcp_port_secure = std::stoi(iter->second);
        }

        iter = headers.find("_tcp_port");
        if (iter != headers.end())
        {
            tcp_port = std::stoi(iter->second);
        }
    }

    bool isValid() const
    {
        return !identity.empty() && !host.empty() && ((http_port > 0 && tcp_port > 0) || (https_port > 0 && tcp_port_secure > 0));
    }

    String string() const
    {
        return "identity=" + identity + ",host=" + host + ",channel=" + channel + ",http_port" + std::to_string(http_port) + ",tcp_port="
            + std::to_string(tcp_port) + ",https_port=" + std::to_string(https_port) + "tcp_port_secure=" + std::to_string(tcp_port_secure);
    }

    Poco::JSON::Object::Ptr json() const
    {
         Poco::JSON::Object::Ptr json_obj(new Poco::JSON::Object());
         json_obj->set("identity", identity);
         json_obj->set("host", host);
         json_obj->set("channel", channel);
         json_obj->set("roles", roles);
         json_obj->set("https_port", https_port);
         json_obj->set("http_port", http_port);
         json_obj->set("tcp_port", tcp_port);
         json_obj->set("tcp_port_secure", tcp_port_secure);
         return json_obj;
    }
};

using NodePtr = std::shared_ptr<Node>;
}
