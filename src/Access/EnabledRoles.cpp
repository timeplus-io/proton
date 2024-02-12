#include <Access/EnabledRoles.h>
#include <Access/Role.h>
#include <Access/EnabledRolesInfo.h>
#include <boost/range/algorithm/copy.hpp>


namespace DB
{
EnabledRoles::EnabledRoles(const Params & params_) : params(params_), handlers(std::make_shared<Handlers>())
{
}

EnabledRoles::~EnabledRoles() = default;


std::shared_ptr<const EnabledRolesInfo> EnabledRoles::getRolesInfo() const
{
    std::lock_guard lock{info_mutex};
    return info;
}


scope_guard EnabledRoles::subscribeForChanges(const OnChangeHandler & handler) const
{
    std::lock_guard lock{handlers->mutex};
    handlers->list.push_back(handler);
    auto it = std::prev(handlers->list.end());

    return [c_handlers=handlers, it]
    {
        std::lock_guard lock2{c_handlers->mutex};
        c_handlers->list.erase(it);
    };
}


void EnabledRoles::setRolesInfo(const std::shared_ptr<const EnabledRolesInfo> & info_, scope_guard * notifications)
{
    {
        std::lock_guard lock{info_mutex};
        if (info && info_ && *info == *info_)
            return;

        info = info_;
    }

    if (notifications)
    {
        std::vector<OnChangeHandler> handlers_to_notify;
        {
            std::lock_guard lock{handlers->mutex};
            boost::range::copy(handlers->list, std::back_inserter(handlers_to_notify));
        }

        notifications->join(scope_guard(
            [c_info = info, c_handlers_to_notify = std::move(handlers_to_notify)]
            {
                for (const auto & handler : c_handlers_to_notify)
                    handler(c_info);
            }));
    }
}

}
